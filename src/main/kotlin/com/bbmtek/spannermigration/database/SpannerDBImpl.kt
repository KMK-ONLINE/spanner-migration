package com.bbmtek.spannermigration.database

import com.bbmtek.spannermigration.Settings
import com.bbmtek.spannermigration.model.ColumnDefinition
import com.bbmtek.spannermigration.model.MigrationUp
import com.bbmtek.spannermigration.model.Migrations
import com.google.cloud.WaitForOption
import com.google.cloud.spanner.*
import java.util.concurrent.TimeUnit

class SpannerDBImpl(private val databaseAdminClient: DatabaseAdminClient,
                    private val databaseClient: DatabaseClient,
                    private val settings: Settings): SpannerDB {
    private val schemaMigrationTableName = "SchemaMigrations"

    override fun createSchemaMigrationsTable() {
        if (!isTableExists(schemaMigrationTableName)) {
            val createSchemaMigrationDdl = """
                CREATE TABLE $schemaMigrationTableName (
                version INT64 NOT NULL,
                ) PRIMARY KEY (version)
                """
            val operation = databaseAdminClient.updateDatabaseDdl(
                    settings.instanceId, settings.databaseId, listOf(createSchemaMigrationDdl), null)
            operation.waitFor(WaitForOption.checkEvery(1L, TimeUnit.SECONDS))
        }
    }

    override fun isTableExists(tableName: String): Boolean {
        return try {
            databaseClient.singleUse().executeQuery(Statement.of("SELECT 1 FROM $tableName")).next()
        } catch (e: SpannerException) {
            false
        }
    }

    override fun getLastMigratedVersion(): Long {
        val resultSet = databaseClient.singleUse().executeQuery(
                Statement.of("SELECT version FROM $schemaMigrationTableName ORDER BY version DESC LIMIT 1"))
        return if (resultSet.next()) {
            resultSet.getLong(0)
        } else {
            -1L
        }
    }

    override fun migrate(migrations: List<Migrations>) {
        migrations.forEach {
            it.up.forEach {
                when(it) {
                    is MigrationUp.CreateTable -> {
                        val migrationDdl = """
                            CREATE TABLE ${it.name} (
                                ${it.columnsToSqlString()}
                            ) PRIMARY KEY (
                                ${it.primaryKeysToSqlString()}
                            )
                        """.trimIndent()
                        databaseAdminClient.updateDatabaseDdl(settings.instanceId, settings.databaseId, listOf(migrationDdl), null)
                                .waitFor(WaitForOption.checkEvery(1L, TimeUnit.SECONDS))
                    }
                    is MigrationUp.AddColumns -> {
                        val tableName = it.name

                        val migrationDDLs = it.columns.map {
                            val columnDefinition = when(it) {
                                is ColumnDefinition.String -> {
                                    "${it.name} ${it.dataType()}(${it.maxLengthString()}) ${it.requiredString()}".removeSuffix(" ")
                                }
                                else -> {
                                    "${it.name} ${it.dataType()} ${it.requiredString()}".removeSuffix(" ")
                                }
                            }

                            """
                                ALTER TABLE $tableName ADD COLUMN $columnDefinition
                            """.trimIndent()
                        }

                        databaseAdminClient.updateDatabaseDdl(settings.instanceId, settings.databaseId, migrationDDLs, null)
                                .waitFor(WaitForOption.checkEvery(1L, TimeUnit.SECONDS))
                    }
                }
            }
            databaseClient.write(
                    listOf(
                            Mutation.newInsertBuilder(schemaMigrationTableName)
                                    .set("version").to(it.version).build()
                    )
            )
        }
    }
}
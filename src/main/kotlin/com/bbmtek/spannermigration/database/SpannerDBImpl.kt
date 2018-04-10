package com.bbmtek.spannermigration.database

import com.bbmtek.spannermigration.Settings
import com.bbmtek.spannermigration.model.ColumnDefinition
import com.bbmtek.spannermigration.model.Migration
import com.bbmtek.spannermigration.model.Migrations
import com.google.cloud.WaitForOption
import com.google.cloud.spanner.*
import java.util.concurrent.TimeUnit

class SpannerDBImpl(private val databaseAdminClient: DatabaseAdminClient,
                    private val databaseClient: DatabaseClient,
                    private val settings: Settings) : SpannerDB {
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
            operation.waitFor(WaitForOption.checkEvery(1L, TimeUnit.SECONDS)).result

            println("$schemaMigrationTableName created.")
        }
    }

    override fun isTableExists(tableName: String): Boolean {
        val resultSet = databaseClient.singleUse().executeQuery(Statement.of("SELECT 1 FROM $tableName"))
        return try {
            resultSet.next()
            true
        } catch (e: SpannerException) {
            false
        } finally {
            resultSet.close()
        }
    }

    override fun getMigratedVersions(): List<Long> {
        val resultSet = databaseClient.singleUse().executeQuery(
                Statement.of("SELECT version FROM $schemaMigrationTableName ORDER BY version DESC"))
        return try {
            val returnList = arrayListOf<Long>()
            while (resultSet.next()){
                returnList.add(resultSet.getLong(0))
            }
            returnList
        } catch (e: SpannerException) {
            listOf<Long>()
        } finally {
            resultSet.close()
        }
    }

    override fun migrate(migrations: List<Migrations>) {
        migrations.forEach {
            it.up.forEach {
                when (it) {
                    is Migration.CreateTable -> {
                        val migrationDdl = """
                            CREATE TABLE ${it.tableName} (
                                ${it.columnsToSqlString()}
                            ) PRIMARY KEY (
                                ${it.primaryKeysToSqlString()}
                            )
                        """.trimIndent()
                        databaseAdminClient.updateDatabaseDdl(settings.instanceId, settings.databaseId, listOf(migrationDdl), null)
                                .waitFor(WaitForOption.checkEvery(1L, TimeUnit.SECONDS))
                                .result

                        println("${it.tableName} table created.")
                    }
                    is Migration.AddColumns -> {
                        val tableName = it.tableName

                        val migrationDDLs = it.columns.map {
                            val columnDefinition = when (it) {
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
                                .result

                        println("Columns ${it.columns.joinToString(",") { it.name }} added to $tableName.")
                    }
                    is Migration.CreateIndex -> {

                        val indexOptions = it.indexOptions.joinToString(" ")

                        val createIndex = """
                            CREATE $indexOptions INDEX ${it.indexName} ON ${it.tableName} (
                                ${it.indexColumnsToSqlString()}
                            )
                        """
                        val storedColumns = if (it.storedColumns.isNotEmpty()) {
                            """
                                STORING (
                                    ${it.storedColumnsToSqlString()}
                                )
                            """
                        } else {
                            ""
                        }

                        val indexDDL = """
                            $createIndex
                            $storedColumns
                        """.trimIndent()


                        databaseAdminClient.updateDatabaseDdl(settings.instanceId, settings.databaseId, listOf(indexDDL), null)
                                .waitFor(WaitForOption.checkEvery(1L, TimeUnit.SECONDS))
                                .result

                        println("Index ${it.indexName} on ${it.tableName} created.")
                    }
                }
            }

            databaseClient.write(
                    listOf(
                            Mutation.newInsertBuilder(schemaMigrationTableName)
                                    .set("version").to(it.version).build()
                    )
            )

            println("Successfuly migrated ${it.version}")
        }
    }
}
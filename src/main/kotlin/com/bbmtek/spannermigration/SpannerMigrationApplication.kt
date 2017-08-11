package com.bbmtek.spannermigration

import com.bbmtek.spannermigration.model.MigrationUp
import com.bbmtek.spannermigration.model.Migrations
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.google.cloud.WaitForOption
import com.google.cloud.spanner.*
import java.io.File
import java.util.concurrent.TimeUnit

/**
 * Created by woi on 10/08/17.
 */
class SpannerMigrationApplication {
    lateinit var dbClient: DatabaseClient
    lateinit var dbAdminClient: DatabaseAdminClient

    private var migrationDir = "${System.getProperty("user.dir")}/examples/migrate"
    private var instanceId = "test-migration"
    private val databaseId = "migration"
    private val schemaMigrationTableName = "SchemaMigrations"

    fun main(args: Array<String>) {
        val options = SpannerOptions.newBuilder().build()
        val spanner = options.getService()
        dbClient = spanner.getDatabaseClient(DatabaseId.of(options.projectId, instanceId, databaseId))
        dbAdminClient = spanner.databaseAdminClient

        if(!checkTableExists(dbClient, schemaMigrationTableName)) {
            val createSchemaMigrationDdl = """
            CREATE TABLE $schemaMigrationTableName (
            version INT64 NOT NULL,
            ) PRIMARY KEY (version)
            """
            val operation = dbAdminClient.updateDatabaseDdl(instanceId, databaseId, listOf(createSchemaMigrationDdl), null)
            operation.waitFor(WaitForOption.checkEvery(1L, TimeUnit.SECONDS))
        }

        val lastVersion = getLastMigratedVersion()

        getMigrations(lastVersion).forEach {
            it.up.forEach {
                when(it) {
                    is MigrationUp.CreateTable -> {
                        val migrationDdl = """
                        CREATE TABLE ${it.name} (
                        ${it.columnsToSqlString()}
                        )
                        PRIMARY KEY (
                        ${it.primaryKeysToSqlString()}
                        )
                        """
                        dbAdminClient.updateDatabaseDdl(instanceId, databaseId, listOf(migrationDdl), null)
                                .waitFor(WaitForOption.checkEvery(1L, TimeUnit.SECONDS))
                    }
                }
            }
            dbClient.write(
                    listOf(
                            Mutation.newInsertBuilder(schemaMigrationTableName)
                                    .set("version").to(it.version).build()
                    )
            )
        }
    }

    private fun checkTableExists(dbClient: DatabaseClient, tableName: String): Boolean {
        try {
            dbClient.singleUse().executeQuery(Statement.of("SELECT 1 FROM $tableName")).next()
            return true
        } catch (e: SpannerException) {
            return false
        }
    }

    private fun getMigrations(lastVersion: Long): List<Migrations> {
        val migrationFiles = File(migrationDir).listFiles()
        val migrations = arrayListOf<Migrations>()
        if(migrationFiles != null) {
            val filteredFiles =  migrationFiles.filter {
                it.nameWithoutExtension.split("_")[0].toLong() > lastVersion
            }

            val objectMapper = ObjectMapper(YAMLFactory())
            filteredFiles.mapTo(migrations) {
                objectMapper.readValue(it, Migrations::class.java)
                        .copy(version = it.nameWithoutExtension.split("_")[0].toLong())
            }
        }
        return migrations
    }

    private fun getLastMigratedVersion(): Long {
        val resultSet = dbClient.singleUse().executeQuery(
                Statement.of("SELECT version FROM $schemaMigrationTableName ORDER BY version DESC LIMIT 1"))
        if (resultSet.next()) {
            return resultSet.getLong(0)
        } else {
            return -1
        }
    }
}
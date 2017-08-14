package com.bbmtek.spannermigration

import com.bbmtek.spannermigration.database.SpannerDB
import com.bbmtek.spannermigration.database.SpannerDBImpl
import com.bbmtek.spannermigration.model.MigrationUp
import com.bbmtek.spannermigration.model.Migrations
import com.beust.jcommander.JCommander
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
    lateinit var settings: Settings
    lateinit var spannerDb: SpannerDB

    private val schemaMigrationTableName = "SchemaMigrations"

    companion object {
        @JvmStatic fun main(vararg argv: String) {
            SpannerMigrationApplication().run(argv)
            System.exit(0)
        }
    }

    fun run(argv: Array<out String>) {
        this.settings = Settings()
        JCommander.newBuilder()
                .addObject(settings)
                .build()
                .parse(*argv)
        val (options, spanner) = initializeSpanner(settings)

        this.dbClient = spanner.getDatabaseClient(DatabaseId.of(options.projectId, settings.instanceId, settings.databaseId))
        this.dbAdminClient = spanner.databaseAdminClient
        this.spannerDb = SpannerDBImpl(dbAdminClient, dbClient, settings)

        spannerDb.createSchemaMigrationsTable()

        val lastVersion = spannerDb.getLastMigratedVersion()

        val migrations = getMigrations(settings.migrationDir, lastVersion)

        migrateDatabase(migrations)
    }

    private fun initializeSpanner(settings: Settings): Pair<SpannerOptions, Spanner> {
        val options = SpannerOptions
                .newBuilder()
                .setProjectId(settings.projectId)
                .build()
        val spanner = options.service
        return Pair(options, spanner)
    }

    private fun migrateDatabase(migrations: List<Migrations>) {
        migrations.forEach {
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
                        dbAdminClient.updateDatabaseDdl(settings.instanceId, settings.databaseId, listOf(migrationDdl), null)
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

    private fun getMigrations(migrationDir: String, lastVersion: Long): List<Migrations> {
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
}
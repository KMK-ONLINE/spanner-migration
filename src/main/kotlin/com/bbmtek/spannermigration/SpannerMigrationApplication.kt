package com.bbmtek.spannermigration

import com.bbmtek.spannermigration.database.SpannerDB
import com.bbmtek.spannermigration.database.SpannerDBImpl
import com.bbmtek.spannermigration.model.Migrations
import com.beust.jcommander.JCommander
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.SpannerOptions
import java.io.File

/**
 * Created by woi on 10/08/17.
 */
class SpannerMigrationApplication {
    lateinit var settings: Settings
    lateinit var spannerDb: SpannerDB

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

        this.spannerDb = SpannerDBImpl(
                spanner.databaseAdminClient,
                databaseClient(spanner, options),
                settings
        )

        spannerDb.createSchemaMigrationsTable()
        spannerDb.getLastMigratedVersion()
                .let { getMigrations(settings.migrationDir, it) }
                .let { spannerDb.migrate(it) }
    }

    private fun databaseClient(spanner: Spanner, options: SpannerOptions): DatabaseClient {
        return spanner.getDatabaseClient(DatabaseId.of(options.projectId, settings.instanceId, settings.databaseId))
    }

    private fun initializeSpanner(settings: Settings): Pair<SpannerOptions, Spanner> {
        val options = SpannerOptions
                .newBuilder()
                .setProjectId(settings.projectId)
                .build()
        val spanner = options.service
        return Pair(options, spanner)
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
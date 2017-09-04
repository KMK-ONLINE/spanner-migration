package com.bbmtek.spannermigration

import com.bbmtek.spannermigration.database.SpannerDB
import com.bbmtek.spannermigration.database.SpannerDBImpl
import com.beust.jcommander.JCommander
import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.SpannerOptions

/**
 * Created by woi on 10/08/17.
 */
class SpannerMigrationApplication {
    private lateinit var settings: Settings
    private lateinit var spannerDb: SpannerDB
    private val migrationLoader = MigrationLoader()

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
                .let { migrationLoader.loadMigrations(settings.migrationDir, it) }
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
}
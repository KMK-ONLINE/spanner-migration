package com.bbmtek.spannermigration.database

import com.bbmtek.spannermigration.Settings
import com.google.cloud.WaitForOption
import com.google.cloud.spanner.DatabaseAdminClient
import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.SpannerException
import com.google.cloud.spanner.Statement
import java.util.concurrent.TimeUnit

class SpannerDBImpl(val databaseAdminClient: DatabaseAdminClient,
                    val databaseClient: DatabaseClient,
                    val settings: Settings): SpannerDB {
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
        try {
            return databaseClient.singleUse().executeQuery(Statement.of("SELECT 1 FROM $tableName")).next()
        } catch (e: SpannerException) {
            return false
        }
    }

    override fun getLastMigratedVersion(): Long {
        val resultSet = databaseClient.singleUse().executeQuery(
                Statement.of("SELECT version FROM $schemaMigrationTableName ORDER BY version DESC LIMIT 1"))
        if (resultSet.next()) {
            return resultSet.getLong(0)
        } else {
            return -1L
        }
    }

}
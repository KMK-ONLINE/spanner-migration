package com.bbmtek.spannermigration.database

/**
 * Created by woi on 14/08/17.
 */
interface SpannerDB {
    fun createSchemaMigrationsTable(): Unit
    fun isTableExists(tableName: String): Boolean
    fun getLastMigratedVersion(): Long
}
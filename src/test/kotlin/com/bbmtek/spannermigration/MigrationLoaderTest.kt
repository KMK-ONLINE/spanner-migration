package com.bbmtek.spannermigration

import org.junit.Assert
import org.junit.Before
import org.junit.Test

/**
 * Created by woi on 04/09/17.
 */
class MigrationLoaderTest {
    lateinit var migrationLoader: MigrationLoader

    @Before
    fun setUp() {
        migrationLoader = MigrationLoader()
    }

    @Test
    fun `should load migration files in correct order`() {
        val migrationsDir = "${System.getProperty("user.dir")}/examples/migrate"
        println("Loaded directory : $migrationsDir")
        val lastVersion = -1L

        val migrations = migrationLoader.loadMigrations(migrationsDir, lastVersion)

        Assert.assertEquals(20170725170000L, migrations.first().version)
        Assert.assertEquals(20170828130000L, migrations.last().version)
    }
}
package com.bbmtek.spannermigration

import org.junit.Assert
import org.junit.Before
import org.junit.Test
import java.util.*
import java.util.Arrays

import org.junit.matchers.JUnitMatchers.*
import org.hamcrest.CoreMatchers.*



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

        val migrations = migrationLoader.loadMigrations(migrationsDir, listOf())

        Assert.assertEquals(20170725170000L, migrations.first().version)
        Assert.assertEquals(20180103170100L, migrations.last().version)
    }

    @Test
    fun `doesn't have any migration`() {
        val migrationsDir = "${System.getProperty("user.dir")}/examples/migrate"
        println("Loaded directory : $migrationsDir")
        val lastVersion = -1L

        val migrations = migrationLoader.loadMigrations(migrationsDir, listOf())
        val migrationVersions = migrations.map { it.version }
        Assert.assertThat(migrationVersions, `is`(equalTo(Arrays.asList(20170725170000L,20170726170000L,20170828130000L))))
    }

    @Test
    fun `already at latest version`() {
        val migrationsDir = "${System.getProperty("user.dir")}/examples/migrate"
        println("Loaded directory : $migrationsDir")
        val lastVersion = -1L

        val migrations = migrationLoader.loadMigrations(migrationsDir, listOf(20170725170000L,20170726170000L,20170828130000L))
        val migrationVersions = migrations.map { it.version }
        Assert.assertEquals(listOf<Long>(), migrationVersions)
    }

    @Test
    fun `still have pending migration at middle`() {
        val migrationsDir = "${System.getProperty("user.dir")}/examples/migrate"
        println("Loaded directory : $migrationsDir")
        val lastVersion = -1L

        val migrations = migrationLoader.loadMigrations(migrationsDir, listOf(20170725170000L,20170828130000L))
        val migrationVersions = migrations.map { it.version }
        Assert.assertEquals(listOf<Long>(20170726170000L), migrationVersions)
    }
}
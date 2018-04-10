package com.bbmtek.spannermigration

import com.google.cloud.WaitForOption
import com.google.cloud.spanner.*
import junit.framework.Assert.assertTrue
import org.hamcrest.Matchers
import org.hamcrest.Matchers.contains
import org.junit.After
import org.junit.Assert.assertThat
import org.junit.Before
import org.junit.Test
import java.util.concurrent.TimeUnit


/**
 * Created by woi on 10/08/17.
 */
class SpannerMigrationApplicationTest {
    companion object {
        private val migrationDir = "${System.getProperty("user.dir")}/examples/migrate"
        private val projectId = "bbm-dev"
        private val instanceId = "bbm-spanner"
        private val databaseId = "migration"
        private val schemaMigrationTableName = "SchemaMigrations"

        private val argv = arrayOf(
                "--migration-dir", migrationDir,
                "--project-id", projectId,
                "--instance-id", instanceId,
                "--database-id", databaseId)
    }

    private val spannerOption = SpannerOptions.newBuilder().setProjectId(projectId).build()
    private val spanner: Spanner = spannerOption.service
    private val dbClient: DatabaseClient = spanner.getDatabaseClient(DatabaseId.of(spannerOption.projectId, instanceId, databaseId))
    private val dbAdminClient: DatabaseAdminClient = spanner.databaseAdminClient

    lateinit var spannerMigrationApplication: SpannerMigrationApplication

    @Before
    fun setup() {
        if (checkTableExists(dbClient, schemaMigrationTableName)) {
            val operation = dbAdminClient.updateDatabaseDdl(instanceId, databaseId, listOf("DROP TABLE $schemaMigrationTableName"), null)
            operation.waitFor(WaitForOption.checkEvery(1L, TimeUnit.SECONDS))
        }

        spannerMigrationApplication = SpannerMigrationApplication()
    }

    @After
    fun teardown() {
        val operation1 = dbAdminClient.updateDatabaseDdl(instanceId, databaseId, listOf("DROP TABLE $schemaMigrationTableName"), null)
        operation1.waitFor(WaitForOption.checkEvery(1L, TimeUnit.SECONDS))
        val operation2 = dbAdminClient.updateDatabaseDdl(instanceId, databaseId, listOf("DROP TABLE Status"), null)
        operation2.waitFor(WaitForOption.checkEvery(1L, TimeUnit.SECONDS))
        val operation3 = dbAdminClient.updateDatabaseDdl(instanceId, databaseId, listOf("DROP TABLE Comments"), null)
        operation3.waitFor(WaitForOption.checkEvery(1L, TimeUnit.SECONDS))
        val operation4 = dbAdminClient.updateDatabaseDdl(instanceId, databaseId, listOf("DROP INDEX IDX_UserStatusLikes_statusUserRegId_statusTimestampDesc_statusUuid"), null)
        operation4.waitFor(WaitForOption.checkEvery(1L, TimeUnit.SECONDS))
        val operation5 = dbAdminClient.updateDatabaseDdl(instanceId, databaseId, listOf("DROP TABLE UserStatusLikes"), null)
        operation5.waitFor(WaitForOption.checkEvery(1L, TimeUnit.SECONDS))
        val operation6 = dbAdminClient.updateDatabaseDdl(instanceId, databaseId, listOf("DROP INDEX NULL_FILTERED_INDEX"), null)
        operation6.waitFor(WaitForOption.checkEvery(1L, TimeUnit.SECONDS))
        val operation7 = dbAdminClient.updateDatabaseDdl(instanceId, databaseId, listOf("DROP TABLE TableWithNullFilteredIndex"), null)
        operation7.waitFor(WaitForOption.checkEvery(1L, TimeUnit.SECONDS))
    }

    @Test
    fun `run for the first time`() {
        spannerMigrationApplication.run(argv)

        val actualDatabaseDdls = dbAdminClient.getDatabaseDdl(instanceId, databaseId)

        val expectedNullFilteredIndexDdl = """
            CREATE NULL_FILTERED INDEX NULL_FILTERED_INDEX ON TableWithNullFilteredIndex(nullableColumn)
        """.trimIndent()

        assert(checkTableExists(dbClient, schemaMigrationTableName))
        assert(checkVersionHaveRow(dbClient))
        assert(checkTableExists(dbClient, "Status"))
        assert(checkTableExists(dbClient, "Comments"))
        assert(checkColumnsExists(dbClient, "Status", arrayOf("likesCount")))
        assert(checkIndexExists(dbClient, "UserStatusLikes", "IDX_UserStatusLikes_statusUserRegId_statusTimestampDesc_statusUuid"))
        assertTrue(actualDatabaseDdls.contains(expectedNullFilteredIndexDdl))
    }

    @Test
    fun `run all migration that has not been executed`() {
        createSchemaTable()
        createStatusTable()

        spannerMigrationApplication.run(argv)

        assert(checkTableExists(dbClient, schemaMigrationTableName))
        assert(checkVersionHaveRow(dbClient))
        assert(checkTableExists(dbClient, "Status"))
        assert(checkTableExists(dbClient, "Comments"))
    }

    @Test
    fun `not running any migration when SchemaMigrations have same versions as migrate folder`() {
        createSchemaTable()
        createStatusTable()
        createCommentsTable()

        spannerMigrationApplication.run(argv)

        assert(checkTableExists(dbClient, schemaMigrationTableName))
        assert(checkVersionHaveRow(dbClient))
        assert(checkTableExists(dbClient, "Status"))
        assert(checkTableExists(dbClient, "Comments"))
    }

    private fun checkTableExists(dbClient: DatabaseClient, tableName: String): Boolean {
        return try {
            dbClient.singleUse().executeQuery(Statement.of("SELECT 1 FROM $tableName")).next()
            true
        } catch (e: SpannerException) {
            false
        }
    }

    private fun checkIndexExists(dbClient: DatabaseClient, tableName: String, indexName: String): Boolean {
        return try {
            dbClient.singleUse().executeQuery(Statement.of("SELECT 1 FROM $tableName@{FORCE_INDEX=$indexName}")).next()
            true
        } catch (e: SpannerException) {
            false
        }
    }

    private fun checkColumnsExists(dbClient: DatabaseClient, tableName: String, columns: Array<String>): Boolean {
        return try {
            dbClient.singleUse().executeQuery(Statement.of("SELECT ${columns.joinToString(",")} FROM $tableName")).next()
            true
        } catch (e: SpannerException) {
            false
        }
    }

    private fun checkVersionHaveRow(dbClient: DatabaseClient): Boolean {
        return try {
            val resultSet = dbClient.singleUse().executeQuery(Statement.of("SELECT COUNT(1) FROM $schemaMigrationTableName"))
            if (resultSet.next()) {
                resultSet.getLong(0) > 0
            } else {
                false
            }
        } catch (e: SpannerException) {
            false
        }
    }

    private fun createSchemaTable() {
        val createSchemaMigrationDdl = """
            CREATE TABLE $schemaMigrationTableName (
            version INT64 NOT NULL,
            ) PRIMARY KEY (version)
            """
        val operation = dbAdminClient.updateDatabaseDdl(instanceId, databaseId, listOf(createSchemaMigrationDdl), null)
        operation.waitFor(WaitForOption.checkEvery(1L, TimeUnit.SECONDS))
    }

    private fun createStatusTable() {
        val migrationDdl = """
                        CREATE TABLE Status (
                            userRegId INT64 NOT NULL,
                            createdAt TIMESTAMP NOT NULL,
                            uuid STRING(64) NOT NULL,
                            newData STRING(MAX)
                        )
                        PRIMARY KEY (
                            userRegId, createdAt DESC, uuid
                        )
                        """
        dbAdminClient.updateDatabaseDdl(instanceId, databaseId, listOf(migrationDdl), null)
                .waitFor(WaitForOption.checkEvery(1L, TimeUnit.SECONDS))
        dbClient.write(
                listOf(
                        Mutation.newInsertBuilder(schemaMigrationTableName)
                                .set("version").to(20170725170000L).build()
                )
        )
    }

    private fun createCommentsTable() {
        val migrationDdl = """
                        CREATE TABLE Comments (
                            userRegId INT64 NOT NULL,
                            createdAt TIMESTAMP NOT NULL,
                            uuid STRING(64) NOT NULL,
                            content STRING(MAX)
                        )
                        PRIMARY KEY (
                            userRegId, createdAt DESC, uuid
                        )
                        """
        dbAdminClient.updateDatabaseDdl(instanceId, databaseId, listOf(migrationDdl), null)
                .waitFor(WaitForOption.checkEvery(1L, TimeUnit.SECONDS))
        dbClient.write(
                listOf(
                        Mutation.newInsertBuilder(schemaMigrationTableName)
                                .set("version").to(20170726170000L).build()
                )
        )
    }
}
package com.bbmtek.spannermigration

import com.google.cloud.WaitForOption
import com.google.cloud.spanner.*
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.util.concurrent.TimeUnit


/**
 * Created by woi on 10/08/17.
 */
class SpannerMigrationApplicationTest {
    lateinit var spanner: Spanner
    lateinit var dbClient: DatabaseClient
    lateinit var dbAdminClient: DatabaseAdminClient
    lateinit var spannerMigrationApplication: SpannerMigrationApplication

    private var migrationDir = "/home/woi/Workspace/spanner-migration/examples/migrate"
    private var projectId = "bbm-dev"
    private var instanceId = "test-migration"
    private val databaseId = "migration"
    private val schemaMigrationTableName = "SchemaMigrations"

    private val argv = arrayOf(
            "--migration-dir", migrationDir,
            "--project-id", projectId,
            "--instance-id", instanceId,
            "--database-id", databaseId)

    @Before
    fun setup() {
        val options = SpannerOptions.newBuilder().build()
        spanner = options.service
        dbClient = spanner.getDatabaseClient(DatabaseId.of(options.projectId, instanceId, databaseId))
        dbAdminClient = spanner.databaseAdminClient

        if(checkTableExists(dbClient, schemaMigrationTableName)) {
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
    }

    @Test
    fun `run for the first time`() {
        spannerMigrationApplication.run(argv)

        assert(checkTableExists(dbClient, schemaMigrationTableName))
        assert(checkVersionHaveRow(dbClient))
        assert(checkTableExists(dbClient, "Status"))
        assert(checkTableExists(dbClient, "Comments"))
        assert(checkColumnsExists(dbClient, "Status", arrayOf("likesCount")))
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

    private fun checkColumnsExists(dbClient: DatabaseClient, tableName: String, columns: Array<String>): Boolean {
        return try {
            dbClient.singleUse().executeQuery(Statement.of("SELECT ${columns.joinToString(",")} FROM $tableName")).next()
            true
        } catch (e: SpannerException) {
            false
        }
    }

    private fun  checkVersionHaveRow(dbClient: DatabaseClient): Boolean {
        return try {
            val resultSet = dbClient.singleUse().executeQuery(Statement.of("SELECT COUNT(1) FROM $schemaMigrationTableName"))
            if(resultSet.next()) {
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
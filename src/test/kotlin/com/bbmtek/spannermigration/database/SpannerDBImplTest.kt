package com.bbmtek.spannermigration.database

import com.bbmtek.spannermigration.Settings
import com.bbmtek.spannermigration.model.ColumnDefinition
import com.bbmtek.spannermigration.model.Migration
import com.bbmtek.spannermigration.model.Migrations
import com.bbmtek.spannermigration.model.PrimaryKeyDefinition
import com.google.cloud.WaitForOption
import com.google.cloud.spanner.*
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata
import org.hamcrest.Matchers
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.Mockito.*
import org.mockito.MockitoAnnotations
import java.util.concurrent.TimeUnit

/**
 * Created by woi on 14/08/17.
 */
class SpannerDBImplTest {
    @Mock
    lateinit var dbAdminClient: DatabaseAdminClient
    @Mock
    lateinit var dbClient: DatabaseClient
    @Mock
    lateinit var mockReadContext: ReadContext
    @Mock
    lateinit var mockResultSet: ResultSet
    @Mock
    lateinit var mockOperation: Operation<Void, UpdateDatabaseDdlMetadata>

    lateinit var settings: Settings
    lateinit var spannerDbImpl: SpannerDBImpl

    @Before
    fun setup() {
        MockitoAnnotations.initMocks(this)
        settings = Settings()
        settings.migrationDir = ""
        settings.projectId = ""
        settings.instanceId = ""
        settings.databaseId = ""
        spannerDbImpl = SpannerDBImpl(dbAdminClient, dbClient, settings)
        `when`(dbClient.singleUse()).thenReturn(mockReadContext)
    }

    @Test
    fun `isTableExists should return true when table exists`() {
        `when`(mockReadContext.executeQuery(any())).thenReturn(mockResultSet)
        `when`(mockResultSet.next()).thenReturn(true, false)

        val isTableExists = spannerDbImpl.isTableExists("SchemaMigrations")
        Assert.assertEquals(true, isTableExists)
    }

    @Test
    fun `isTableExists should return false when table not exists`() {
        `when`(mockReadContext.executeQuery(any())).thenReturn(mockResultSet)
        `when`(mockResultSet.next()).thenThrow(SpannerExceptionFactory.newSpannerException(ErrorCode.INVALID_ARGUMENT, "Table not exists"))

        val isTableExists = spannerDbImpl.isTableExists("SchemaMigrations")
        Assert.assertEquals(false, isTableExists)
    }


    @Test
    fun `getMigratedVersions should return 20170725170000 when have migration on SchemaMigrations`() {
        `when`(mockReadContext.executeQuery(any())).thenReturn(mockResultSet)
        `when`(mockResultSet.next()).thenReturn(true, false)
        `when`(mockResultSet.getLong(0)).thenReturn(20170725170000L)

        val migratedVersions = spannerDbImpl.getMigratedVersions()
        Assert.assertThat(migratedVersions, Matchers.`is`(Matchers.equalTo(listOf(20170725170000L))))
    }

    @Test
    fun `getMigratedVersions should return -1 when no migration on SchemaMigrations`() {
        `when`(mockReadContext.executeQuery(any())).thenReturn(mockResultSet)
        `when`(mockResultSet.next()).thenReturn(false)

        val migratedVersions = spannerDbImpl.getMigratedVersions()
        Assert.assertThat(migratedVersions, Matchers.`is`(Matchers.equalTo(listOf<Long>())))
    }

    @Test
    fun `createSchemaMigration should call updateDatabaseDdl if SchemaMigrations table is not exists`() {
        `when`(mockReadContext.executeQuery(any())).thenReturn(mockResultSet)
        `when`(mockResultSet.next()).thenThrow(SpannerExceptionFactory.newSpannerException(ErrorCode.INVALID_ARGUMENT, "Table not exists"))
        `when`(dbAdminClient.updateDatabaseDdl(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), any(), eq(null)))
                .thenReturn(mockOperation)
        `when`(mockOperation.waitFor(ArgumentMatchers.any())).thenReturn(mockOperation)

        spannerDbImpl.createSchemaMigrationsTable()

        verify(dbAdminClient, atLeastOnce()).updateDatabaseDdl(
                ArgumentMatchers.anyString(),
                ArgumentMatchers.anyString(),
                any(),
                eq(null))
        verify(mockOperation, atLeastOnce()).waitFor(eq(WaitForOption.checkEvery(1L, TimeUnit.SECONDS)))
    }

    @Test
    fun `createSchemaMigration should do nothing if SchemaMigrations table is exists`() {
        `when`(mockReadContext.executeQuery(any())).thenReturn(mockResultSet)
        `when`(mockResultSet.next()).thenReturn(true, false)
        `when`(dbAdminClient.updateDatabaseDdl(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), any(), eq(null)))
                .thenReturn(mockOperation)

        spannerDbImpl.createSchemaMigrationsTable()

        verify(dbAdminClient, never()).updateDatabaseDdl(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), any(), eq(null))
    }

    @Test
    fun `migrate database`() {
        `when`(dbAdminClient.updateDatabaseDdl(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), any(), eq(null)))
                .thenReturn(mockOperation)
        `when`(mockOperation.waitFor(ArgumentMatchers.any())).thenReturn(mockOperation)
        `when`(mockOperation.isSuccessful).thenReturn(true)

        val migrations = listOf<Migrations>(Migrations(up = listOf(
                Migration.CreateTable(
                        tableName = "Status",
                        columns = listOf(
                                ColumnDefinition.Int64("userRegId", true),
                                ColumnDefinition.Timestamp("timestamp", true),
                                ColumnDefinition.String("status", true),
                                ColumnDefinition.String("newData"),
                                ColumnDefinition.Bool("isDeleted", true)
                        ),
                        primaryKeys = listOf(
                                PrimaryKeyDefinition("isDeleted"),
                                PrimaryKeyDefinition("userRegId"),
                                PrimaryKeyDefinition("timestamp", "DESC")
                        )
                ),
                Migration.AddColumns(
                        tableName = "Status",
                        columns = listOf(
                                ColumnDefinition.Int64("likesCount"),
                                ColumnDefinition.Bool("isUpdated")
                        )
                )
        ), version = 1L))

        val expectedCreateDDL = """
                CREATE TABLE Status (
                    userRegId Int64 NOT NULL,timestamp Timestamp NOT NULL,status String(MAX) NOT NULL,newData String(MAX),isDeleted Bool NOT NULL
                ) PRIMARY KEY (
                    isDeleted ,userRegId ,timestamp DESC
                )
            """.trimIndent()

        val expectedAlterDDLOne = """
                ALTER TABLE Status ADD COLUMN likesCount Int64
            """.trimIndent()

        val expectedAlterDDLTwo = """
                ALTER TABLE Status ADD COLUMN isUpdated Bool
            """.trimIndent()

        spannerDbImpl.migrate(migrations)

        Mockito.verify(dbAdminClient).updateDatabaseDdl(settings.instanceId, settings.projectId, listOf(expectedCreateDDL), null)
        Mockito.verify(dbAdminClient).updateDatabaseDdl(settings.instanceId, settings.projectId, listOf(expectedAlterDDLOne, expectedAlterDDLTwo), null)
        Mockito.verify(dbClient).write(ArgumentMatchers.anyList())
    }
}
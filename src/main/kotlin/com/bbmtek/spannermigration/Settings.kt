package com.bbmtek.spannermigration

import com.beust.jcommander.Parameter

/**
 * Created by woi on 14/08/17.
 */
class Settings {
    @Parameter
    var parameters: ArrayList<String> = arrayListOf()

    @Parameter(names = arrayOf("--migration-dir", "-m"), required = true, description = "Migration directory containing JSON files")
    lateinit var migrationDir: String

    @Parameter(names = arrayOf("--project-id", "-p"), required = true, description = "Google Cloud Project ID")
    lateinit var projectId: String

    @Parameter(names = arrayOf("--instance-id", "-i"), required = true, description = "Spanner Instance ID")
    lateinit var instanceId: String

    @Parameter(names = arrayOf("--database-id", "-d"), required = true, description = "Spanner Database ID")
    lateinit var databaseId: String
}
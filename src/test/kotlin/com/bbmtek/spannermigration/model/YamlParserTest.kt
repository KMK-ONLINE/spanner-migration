package com.bbmtek.spannermigration.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.junit.Test

/**
 * Created by woi on 11/08/17.
 */
class YamlParserTest {
    @Test
    fun `convertYamlToObject`() {
        val mapper = ObjectMapper(YAMLFactory())
        val stringYaml = """
up:
  - type: CreateTable
    tableName: Status
    columns:
       - type: Int64
         tableName: userRegId
         required: true
       - type: Timestamp
         tableName: createdAt
         required: true
       - type: String
         tableName: newData
         maxLength: 160
       - type: Bool
         tableName: isDeleted
         required: true
  - type: AddColumns
    tableName: Status
    columns:
      - type: Int64
        tableName: likesCount
                        """
        val migrations = mapper.readValue(stringYaml, Migrations::class.java)
        println(migrations.toString())
    }
}
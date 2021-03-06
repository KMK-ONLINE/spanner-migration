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
         name: userRegId
         required: true
       - type: Timestamp
         name: createdAt
         required: true
       - type: String
         name: newData
         maxLength: 160
       - type: Bool
         name: isDeleted
         required: true
  - type: AddColumns
    tableName: Status
    columns:
      - type: Int64
        name: likesCount
                        """
        mapper.readValue(stringYaml, Migrations::class.java)
    }
}
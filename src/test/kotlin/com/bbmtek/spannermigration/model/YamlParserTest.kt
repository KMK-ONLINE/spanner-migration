package com.bbmtek.spannermigration.model

import com.fasterxml.jackson.databind.DeserializationConfig
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.junit.Test
import org.junit.Assert.assertEquals

/**
 * Created by woi on 11/08/17.
 */
class YamlParserTest {

    @Test
    fun `convertYamlToObject`() {
        println("Int64 : ${ColumnDefinition.Int64::class.java.canonicalName}")
        val mapper = ObjectMapper(YAMLFactory())
        val stringYaml ="""
up:
  - type: CreateTable
    name: Status
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
                        """
        val migrations = mapper.readValue(stringYaml, Migrations::class.java)
        println(migrations.toString())
    }
}
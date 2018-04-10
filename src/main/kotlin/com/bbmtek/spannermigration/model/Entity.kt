package com.bbmtek.spannermigration.model

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonValue



/**
 * Created by woi on 11/08/17.
 */
data class Migrations(val up: List<Migration> = listOf(), val version: Long = 0)

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
        JsonSubTypes.Type(value = Migration.CreateTable::class, name = "CreateTable"),
        JsonSubTypes.Type(value = Migration.AddColumns::class, name = "AddColumns"),
        JsonSubTypes.Type(value = Migration.CreateIndex::class, name = "CreateIndex")
)
sealed class Migration {
    data class CreateTable(
            var tableName: kotlin.String = "",
            var columns: List<ColumnDefinition> = listOf(),
            var primaryKeys: List<PrimaryKeyDefinition> = listOf()
    ) : Migration() {
        fun columnsToSqlString(): kotlin.String {
            return this.columns
                    .map {
                        when (it) {
                            is ColumnDefinition.String -> {
                                "${it.name} ${it.dataType()}(${it.maxLengthString()}) ${it.requiredString()}".removeSuffix(" ")
                            }
                            else -> {
                                "${it.name} ${it.dataType()} ${it.requiredString()}".removeSuffix(" ")
                            }
                        }
                    }
                    .joinToString(",")
        }

        fun primaryKeysToSqlString(): kotlin.String {
            return this.primaryKeys.joinToString(",") { "${it.name} ${it.order}" }
        }
    }

    data class AddColumns(
            var tableName: kotlin.String = "",
            var columns: List<ColumnDefinition> = listOf()
    ) : Migration()

    data class CreateIndex(
            var indexName: kotlin.String = "",
            var tableName: kotlin.String = "",
            var indexOptions: List<IndexOption> = listOf(),
            var indexColumns: List<IndexKeyDefinition> = listOf(),
            var storedColumns: List<String> = listOf()
    ): Migration() {
        fun indexColumnsToSqlString(): kotlin.String {
            return this.indexColumns.joinToString(",") { "${it.name} ${it.order}" }
        }

        fun storedColumnsToSqlString(): kotlin.String {
            return this.storedColumns.joinToString(",")
        }
    }
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(JsonSubTypes.Type(value = ColumnDefinition.Int64::class, name = "Int64"), JsonSubTypes.Type(value = ColumnDefinition.Timestamp::class, name = "Timestamp"), JsonSubTypes.Type(value = ColumnDefinition.String::class, name = "String"), JsonSubTypes.Type(value = ColumnDefinition.Bool::class, name = "Bool"))
sealed class ColumnDefinition {
    abstract var name: kotlin.String
    abstract var required: Boolean

    data class Int64(
            override var name: kotlin.String = "",
            override var required: Boolean = false
    ) : ColumnDefinition()

    data class String(
            override var name: kotlin.String = "",
            override var required: Boolean = false,
            var maxLength: Int? = null
    ) : ColumnDefinition() {
        fun maxLengthString(): kotlin.String = when (maxLength) {
            is Int -> maxLength.toString()
            else -> "MAX"
        }
    }

    data class Timestamp(
            override var name: kotlin.String = "",
            override var required: Boolean = false
    ) : ColumnDefinition()

    data class Bool(
            override var name: kotlin.String = "",
            override var required: Boolean = false
    ) : ColumnDefinition()

    fun dataType(): kotlin.String {
        return this::class.java.simpleName
    }

    fun requiredString(): kotlin.String {
        return if (required) {
            "NOT NULL"
        } else {
            ""
        }
    }
}

data class PrimaryKeyDefinition(
        var name: kotlin.String = "",
        var order: kotlin.String = ""
)

data class IndexKeyDefinition(
        var name: kotlin.String = "",
        var order: kotlin.String = ""
)

enum class IndexOption {
    UNIQUE, NULL_FILTERED;

    @JsonValue
    fun toValue(): String {
        return this.name
    }
}

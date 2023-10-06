package com.cjuega.easydynamodb.config

import software.amazon.awssdk.services.dynamodb.model.AttributeValue

data class DynamoDbPrimaryKey(val partitionKey: String, val sortKey: String?)

class DynamoDbConfig(
    private val tableName: String,
    private val primaryKey: DynamoDbPrimaryKey,
    private val indexes: Map<String, DynamoDbPrimaryKey>
) {
    fun tableName() = tableName

    fun primaryKey() = primaryKey.copy()

    fun indexes() = indexes.toMap()

    fun extractPrimaryKey(item: Map<String, AttributeValue>): Map<String, AttributeValue> {
        val primaryAttributes = listOfNotNull(primaryKey.partitionKey, primaryKey.sortKey)

        return item.filterKeys { it in primaryAttributes }
    }

    fun extractAllKeys(item: Map<String, AttributeValue>): Map<String, AttributeValue> {
        val primaryAttributes = listOfNotNull(primaryKey.partitionKey, primaryKey.sortKey)
        val indexAttributes = indexes.flatMap { listOfNotNull(it.value.partitionKey, it.value.sortKey) }

        return item.filterKeys { it in primaryAttributes || it in indexAttributes }
    }
}
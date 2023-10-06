package com.cjuega.easydynamodb

import com.cjuega.easydynamodb.config.DynamoDbConfig
import com.cjuega.easydynamodb.config.DynamoDbPrimaryKey
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*

class DynamoDbEnvironmentArranger(private val config: DynamoDbConfig, private val client: DynamoDbClient) {
    fun arrange() {
        createTableIfNeeded()
        deleteAllItems()
    }

    private fun createTableIfNeeded() {
        try {
            client.describeTable {
                it.tableName(config.tableName())
            }
        } catch (e: ResourceNotFoundException) {
            createTable()
        }
    }

    private fun createTable() {
        client.createTable {
            it.tableName(config.tableName())
            it.attributeDefinitions(attributeDefinitionsOf(config))
            it.keySchema(keySchemaOf(config))
            it.globalSecondaryIndexes(globalSecondaryIndexesOf(config))
            it.billingMode(BillingMode.PAY_PER_REQUEST)
        }
    }

    private fun deleteAllItems() {
        var start: Map<String, AttributeValue>? = null

        do {
            client.scan {
                it.tableName(config.tableName())
                it.exclusiveStartKey(start)
            }.let {
                start = if (it.hasLastEvaluatedKey() && it.lastEvaluatedKey().isNotEmpty()) it.lastEvaluatedKey() else null
                deleteItems(it.items())
            }
        } while (start != null)
    }

    private fun deleteItems(items: List<Map<String, AttributeValue>>) {
        if (items.isNotEmpty()) {
            items.chunked(BATCH_WRITE_SIZE_LIMIT).forEach {
                deleteItemsInBatch(it)
            }
        }
    }

    private fun deleteItemsInBatch(items: List<Map<String, AttributeValue>>) {
        client.batchWriteItem {
            it.requestItems(mapOf(config.tableName() to items.map { item ->
                WriteRequest.builder()
                    .deleteRequest(DeleteRequest.builder().key(config.extractPrimaryKey(item)).build())
                    .build()
            }))
        }
    }

    companion object {
        private const val BATCH_WRITE_SIZE_LIMIT = 25

        private fun attributeDefinitionsOf(config: DynamoDbConfig): List<AttributeDefinition> {
            return listOf(
                listOf(config.primaryKey()),
                config.indexes().map { it.value }
            ).flatten()
            .map {
                val pk = AttributeDefinition.builder()
                    .attributeName(it.partitionKey)
                    .attributeType(ScalarAttributeType.S)
                    .build()

                val sk = if (it.sortKey != null) AttributeDefinition.builder()
                    .attributeName(it.sortKey)
                    .attributeType(ScalarAttributeType.S)
                    .build() else null

                listOf(pk, sk)
            }.flatten()
            .filterNotNull()
        }

        private fun keySchemaOf(config: DynamoDbConfig): List<KeySchemaElement> {
            return keySchemaOf(config.primaryKey())
        }

        private fun keySchemaOf(key: DynamoDbPrimaryKey): List<KeySchemaElement> {
            return buildList {
                this.add(
                    KeySchemaElement.builder()
                        .keyType(KeyType.HASH)
                        .attributeName(key.partitionKey)
                        .build()
                )

                if (key.sortKey != null) {
                    this.add(
                        KeySchemaElement.builder()
                            .keyType(KeyType.RANGE)
                            .attributeName(key.sortKey)
                            .build()
                    )
                }
            }
        }

        private fun globalSecondaryIndexesOf(config: DynamoDbConfig): List<GlobalSecondaryIndex> {
            return config.indexes().map { (indexName, primaryKey) ->
                GlobalSecondaryIndex.builder()
                    .indexName(indexName)
                    .keySchema(keySchemaOf(primaryKey))
                    .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(1).writeCapacityUnits(1).build())
                    .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                    .build()
            }
        }
    }
}

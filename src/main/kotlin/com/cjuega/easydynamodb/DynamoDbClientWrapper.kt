package com.cjuega.easydynamodb

import com.cjuega.easydynamodb.config.DynamoDbConfig
import com.cjuega.easydynamodb.expressions.DynamoDbExpressionsBuilder
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*

class DynamoDbClientWrapper(
    private val config: DynamoDbConfig,
    private val client: DynamoDbClient,
    private val maxRetries: Int = 10
) {

    companion object {
        private const val BATCH_WRITE_SIZE_LIMIT = 25
        private const val BATCH_GET_SIZE_LIMIT = 100
        private const val RETRY_TIMEOUT_IN_MILLISECONDS = 500L

        private fun ensureValidStartCursors(totalSegments: Int, start: List<Map<String, AttributeValue>?>?) {
            if (start != null && start.size != totalSegments) {
                throw IllegalArgumentException("The number of start cursors must match the number of segments")
            }
        }
    }

    fun getItem(key: Map<String, AttributeValue>): Map<String, AttributeValue>? {
        val response = client.getItem {
            it.tableName(config.tableName())
            it.key(config.extractPrimaryKey(key))
        }

        if (!response.hasItem() || response.item().isEmpty()) {
            return null
        }

        return response.item()
    }

    fun putItem(item: Map<String, AttributeValue>, condition: String? = null) {
        client.putItem {
            it.tableName(config.tableName())
            it.item(item)

            DynamoDbExpressionsBuilder.parseConditionExpression(condition)?.let { exp ->
                it.conditionExpression(exp.expression)
                it.expressionAttributeNames(exp.attributeNames)
                it.expressionAttributeValues(exp.attributeValues)
            }
        }
    }

    suspend fun batchWrite(items: List<WriteRequest>) {
        val batches = items.chunked(BATCH_WRITE_SIZE_LIMIT)

        val promises = coroutineScope {
            batches.map { async { limitedBatchWrite(it) } }
        }

        promises.awaitAll()
    }

    private suspend fun limitedBatchWrite(items: List<WriteRequest>) {
        val builder = BatchWriteItemRequest.builder()
            .requestItems(mapOf(config.tableName() to items))

        var areMoreItems = false

        val unsafeAction: () -> Unit = {
            val response = client.batchWriteItem(builder.build())

            areMoreItems = response.hasUnprocessedItems() && response.unprocessedItems().isNotEmpty()

            if (areMoreItems) {
                builder.requestItems(response.unprocessedItems())
            }
        }

        do {
            withRetries(unsafeAction, 0)
        } while (areMoreItems)
    }

    private suspend fun withRetries(action: () -> Unit, retries: Int) {
        return try {
            action()
        } catch (e: ProvisionedThroughputExceededException) {
            if (retries < maxRetries) {
                delay(RETRY_TIMEOUT_IN_MILLISECONDS)
                withRetries(action, retries + 1)
            } else {
                throw e
            }
        }
    }

    suspend fun batchGet(keys: List<Map<String, AttributeValue>>): List<Map<String, AttributeValue>> {
        val batches = keys.chunked(BATCH_GET_SIZE_LIMIT)

        val promises = coroutineScope {
            batches.map { async { limitedBatchGet(it) } }
        }

        return promises.awaitAll().flatten()
    }

    private suspend fun limitedBatchGet(keys: List<Map<String, AttributeValue>>): List<Map<String, AttributeValue>> {
        val builder = BatchGetItemRequest.builder()
            .requestItems(mapOf(config.tableName() to KeysAndAttributes.builder().keys(keys).build()))

        var areMoreItems = false

        val items: MutableList<Map<String, AttributeValue>> = mutableListOf()

        val unsafeAction: () -> Unit = {
            val response = client.batchGetItem(builder.build())

            response.responses()[config.tableName()]?.let {
                items.addAll(it)
            }

            areMoreItems = response.hasUnprocessedKeys() && response.unprocessedKeys().isNotEmpty()

            if (areMoreItems) {
                builder.requestItems(response.unprocessedKeys())
            }
        }

        do {
            withRetries(unsafeAction, 0)
        } while (areMoreItems)

        return items
    }

    suspend fun query(
        keyExpression: String,
        action: (List<Map<String, AttributeValue>>, prev: Map<String, AttributeValue>?, next: Map<String, AttributeValue>?) -> Unit,
        indexName: String? = null,
        where: String? = null,
        scanForward: Boolean? = true,
        start: Map<String, AttributeValue>? = null,
        limit: Int? = null
    ) {
        val builder = QueryRequest.builder()
            .tableName(config.tableName())
            .scanIndexForward(scanForward)

        val keyExp = DynamoDbExpressionsBuilder.parseKeyExpression(keyExpression)
        val filterExp = DynamoDbExpressionsBuilder.parseFilterExpression(where, keyExp)

        builder.keyConditionExpression(keyExp.expression)
        builder.expressionAttributeNames(filterExp?.attributeNames ?: keyExp.attributeNames)
        builder.expressionAttributeValues(filterExp?.attributeValues ?: keyExp.attributeValues)

        if (filterExp != null) {
            builder.filterExpression(filterExp.expression)
        }

        if (indexName != null) {
            builder.indexName(indexName)
        }

        if (start != null) {
            builder.exclusiveStartKey(extractKeys(start, indexName))
        }

        var bLimit = 0

        if (limit != null) {
            bLimit = limit
            builder.limit(bLimit)
        }

        var prev = start
        var areMoreItems = false

        val unsafeAction: () -> Unit = {
            val response = client.query(builder.build())

            areMoreItems = response.hasLastEvaluatedKey() && response.lastEvaluatedKey().isNotEmpty()

            if (prev == null && response.hasItems() && response.items().isNotEmpty()) {
                prev = response.items()[0]
            }

            action(
                response.items(),
                extractKeys(prev, indexName),
                if (areMoreItems) extractKeys(response.lastEvaluatedKey(), indexName) else null
            )

            if (areMoreItems) {
                builder.exclusiveStartKey(response.lastEvaluatedKey())
                if (limit != null) {
                    bLimit -= response.items().size
                    areMoreItems = bLimit > 0
                    builder.limit(bLimit)
                }
            }
        }

        do {
            withRetries(unsafeAction, 0)
        } while (areMoreItems)
    }

    private fun extractKeys(item: Map<String, AttributeValue>?, indexName: String?): Map<String, AttributeValue>? {
        if (item == null) {
            return null
        }

        return if (indexName == null) {
            config.extractPrimaryKey(item)
        } else
            config.extractAllKeys(item)
    }

    suspend fun scan(
        action: (List<Map<String, AttributeValue>>, Map<String, AttributeValue>?) -> Unit,
        indexName: String? = null,
        where: String? = null,
        start: Map<String, AttributeValue>? = null,
        limit: Int? = null
    ) = parallelScan(
        1,
        { items, _, next -> action(items, next) },
        indexName,
        where,
        if (start != null) listOf(start) else null,
        limit
    )

    suspend fun parallelScan(
        totalSegments: Int,
        action: (List<Map<String, AttributeValue>>, Int, Map<String, AttributeValue>?) -> Unit,
        indexName: String? = null,
        where: String? = null,
        start: List<Map<String, AttributeValue>?>? = null,
        limit: Int? = null
    ) {
        ensureValidStartCursors(totalSegments, start)

        val promises = coroutineScope {
            (0..<totalSegments).map { i ->
                async {
                    val lastEvaluatedKey = start?.get(i)
                    if (start == null || lastEvaluatedKey != null) {
                        scanSegment(i, totalSegments, action, indexName, where, lastEvaluatedKey, limit)
                    }
                }
            }
        }

        promises.awaitAll()
    }

    private suspend fun scanSegment(
        segment: Int,
        totalSegments: Int,
        action: (List<Map<String, AttributeValue>>, Int, Map<String, AttributeValue>?) -> Unit,
        indexName: String?,
        where: String? = null,
        start: Map<String, AttributeValue>? = null,
        limit: Int? = null
    ) {
        val builder = ScanRequest.builder()
            .tableName(config.tableName())
            .segment(segment)
            .totalSegments(totalSegments)

        if (indexName != null) {
            builder.indexName(indexName)
        }

        DynamoDbExpressionsBuilder.parseFilterExpression(where)?.let {
            builder.filterExpression(it.expression)
            builder.expressionAttributeNames(it.attributeNames)
            builder.expressionAttributeValues(it.attributeValues)
        }

        if (start != null) {
            builder.exclusiveStartKey(extractKeys(start, indexName))
        }

        var bLimit = 0

        if (limit != null) {
            bLimit = limit
            builder.limit(bLimit)
        }

        var areMoreItems = false

        val unsafeAction: () -> Unit = {
            val response = client.scan(builder.build())

            areMoreItems = response.hasLastEvaluatedKey() && response.lastEvaluatedKey().isNotEmpty()

            action(
                response.items(),
                segment,
                if (areMoreItems) extractKeys(response.lastEvaluatedKey(), indexName) else null
            )

            if (areMoreItems) {
                builder.exclusiveStartKey(response.lastEvaluatedKey())
                if (limit != null) {
                    bLimit -= response.items().size
                    areMoreItems = bLimit > 0
                    builder.limit(bLimit)
                }
            }
        }

        do {
            withRetries(unsafeAction, 0)
        } while (areMoreItems)
    }
}
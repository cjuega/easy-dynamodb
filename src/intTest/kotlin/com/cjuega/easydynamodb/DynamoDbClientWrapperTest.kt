package com.cjuega.easydynamodb

import com.cjuega.easydynamodb.config.DynamoDbConfig
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.WriteRequest

class DynamoDbClientWrapperTest {
    private lateinit var config: DynamoDbConfig
    private lateinit var client: DynamoDbClient
    private lateinit var arranger: DynamoDbEnvironmentArranger
    private lateinit var testee: DynamoDbClientWrapper

    @BeforeEach
    fun setUp() {
        config = TestDynamoDbConfigProvider.config()
        client = DynamoDbClient.builder()
            .endpointOverride(DynamoDbContainerProperties.getDynamoDbProperties())
            .build()
        arranger = DynamoDbEnvironmentArranger(config, client)
        testee = DynamoDbClientWrapper(config, client)

        arranger.arrange()
    }

    @Nested
    @DisplayName("putItem")
    inner class PutItemTest {
        @Test
        fun `should persist an item in the table when the item didn´t exist`(): Unit = runBlocking {
            // Given
            val item = randomItems(1)[0]

            // When
            testee.putItem(item)

            // Then
            val actual = scan()
            assertThat(actual).containsExactly(item)
        }

        @Test
        fun `should overwrite an item in the table when it already existed`(): Unit = runBlocking {
            // Given
            val item = randomItems(1)[0]
            testee.putItem(item)

            val overwrite = item.toMutableMap()
            overwrite["Value"] = AttributeValue.builder().n("${ObjectMother.int()}").build()

            // When
            testee.putItem(overwrite.toMap())

            // Then
            val actual = scan()
            assertThat(actual).containsExactly(overwrite)
        }

        @Test
        fun `should persist an item when the condition is satisfied`(): Unit = runBlocking {
            // Given
            val item = randomItems(1)[0]

            // When
            testee.putItem(item, "attribute_not_exists(Version) or Type = \"Random\"")

            // Then
            val actual = scan()
            assertThat(actual).containsExactly(item)
        }

        @Test
        fun `should throw a ConditionalCheckFailedException exception when the condition is not met`(): Unit =
            runBlocking {
                val item = randomItems(1)[0]

                assertThrows<ConditionalCheckFailedException> {
                    testee.putItem(item, "attribute_exists(SK)")
                }
            }
    }

    @Nested
    @DisplayName("batchWrite")
    inner class BatchWriteTest {
        @Test
        fun `should write multiple items in batch`(): Unit = runBlocking {
            // Given
            val items = randomItems(200)

            // When
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // Then
            // no exception launched
        }

        @Test
        fun `should support puts and deletes in the same request`(): Unit = runBlocking {
            // Given
            val items = randomItems(200)
            val expected = items.filterIndexed { index, _ -> index % 2 == 1 }
            var writeRequests = items
                .filterIndexed { index, _ -> index % 2 == 0 }
                .map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }

            testee.batchWrite(writeRequests)

            // When
            writeRequests = items.mapIndexed { index, item ->
                if (index % 2 == 0) {
                    WriteRequest.builder().deleteRequest { it.key(config.extractPrimaryKey(item)) }.build()
                } else {
                    WriteRequest.builder().putRequest { it.item(item) }.build()
                }
            }
            testee.batchWrite(writeRequests)

            // Then
            val actual = scan()
            assertThat(actual).hasSameElementsAs(expected)
        }

        @Test
        fun `should do nothing when deleting items that do not exist`(): Unit = runBlocking {
            // Given
            val items = randomItems(10)

            // When
            val writeRequests = items.map { item ->
                WriteRequest.builder().deleteRequest { it.key(config.extractPrimaryKey(item)) }.build()
            }
            testee.batchWrite(writeRequests)

            // Then
            // no exception launched
        }
    }

    @Nested
    @DisplayName("batchGet")
    inner class BatchGetTest {
        @Test
        fun `should return nothing when items do not exist`(): Unit = runBlocking {
            // Given
            val items = randomItems(200)

            // When
            val keys = items.map { config.extractPrimaryKey(it) }
            val actual = testee.batchGet(keys)

            // Then
            assertThat(actual).isEmpty()
        }

        @Test
        fun `should return all items when all exist`(): Unit = runBlocking {
            // Given
            val items = randomItems(200)
            val expected = items.toList()
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val keys = items.map { config.extractPrimaryKey(it) }
            val actual = testee.batchGet(keys)

            // Then
            assertThat(actual).hasSameElementsAs(expected)
        }

        @Test
        fun `should return only existing items`(): Unit = runBlocking {
            // Given
            val items = randomItems(200)
            val expected = items.filterIndexed { index, _ -> index % 2 == 0 }
            val writeRequests = items
                .filterIndexed { index, _ -> index % 2 == 0 }
                .map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val keys = items.map { config.extractPrimaryKey(it) }
            val actual = testee.batchGet(keys)

            // Then
            assertThat(actual).hasSameElementsAs(expected)
        }
    }

    @Nested
    @DisplayName("query")
    inner class QueryTest {
        private fun randomItemCollection(partitionKey: String, size: Int? = 100): List<Map<String, AttributeValue>> {
            return (1..size!!).map { i ->
                mapOf(
                    "PK" to AttributeValue.builder().s(partitionKey).build(),
                    "SK" to AttributeValue.builder().s("SK#$i").build(),
                    "Type" to AttributeValue.builder().s("Random").build(),
                    "IsValid" to AttributeValue.builder().bool(ObjectMother.coin()).build(),
                    "Value" to AttributeValue.builder().n("${ObjectMother.int()}").build(),
                    "GSI1PK" to AttributeValue.builder().s("SK#$i").build(),
                    "GSI1SK" to AttributeValue.builder().s(partitionKey).build()
                )
            }
        }

        @Test
        fun `should return nothing when table is empty`(): Unit = runBlocking {
            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val keyExpression = "PK = \"PK#1\" and begins_with(SK, \"SK#\")"
            val action: (List<Map<String, AttributeValue>>, prev: Map<String, AttributeValue>?, next: Map<String, AttributeValue>?) -> Unit =
                { list, _, _ ->
                    actual.addAll(list)
                }
            testee.query(keyExpression = keyExpression, action = action)

            // Then
            assertThat(actual).isEmpty()
        }

        @Test
        fun `should return nothing when GSI is empty`(): Unit = runBlocking {
            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val keyExpression = "GSI1PK = \"SK#1\" and begins_with(GSI1SK, \"PK#\")"
            val action: (List<Map<String, AttributeValue>>, prev: Map<String, AttributeValue>?, next: Map<String, AttributeValue>?) -> Unit =
                { list, _, _ ->
                    actual.addAll(list)
                }
            testee.query(indexName = "GSI1", keyExpression = keyExpression, action = action)

            // Then
            assertThat(actual).isEmpty()
        }

        @Test
        fun `should return all items in the item collection matching the key expression`(): Unit = runBlocking {
            // Given
            val partitionKey = "PK#${ObjectMother.int()}"
            val items = randomItemCollection(partitionKey, 100)
            val expected = items.sortedBy { it["SK"]!!.s() }.toList()
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val keyExpression = "PK = \"$partitionKey\" and begins_with(SK, \"SK#\")"
            val action: (List<Map<String, AttributeValue>>, prev: Map<String, AttributeValue>?, next: Map<String, AttributeValue>?) -> Unit =
                { list, _, _ ->
                    actual.addAll(list)
                }
            testee.query(keyExpression = keyExpression, action = action)

            // Then
            assertThat(actual).isEqualTo(expected)
        }

        @Test
        fun `should return items in the item collection filtered by key expression and where clause`(): Unit =
            runBlocking {
                // Given
                val partitionKey = "PK#${ObjectMother.int()}"
                val items = randomItemCollection(partitionKey, 100)
                val expected = items
                    .sortedBy { it["SK"]!!.s() }
                    .filter { it["SK"]!!.s()!! >= "SK#1" && it["SK"]!!.s()!! <= "SK#50" }
                    .filter { it["IsValid"]!!.bool() == true }
                    .toList()
                val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
                testee.batchWrite(writeRequests)

                // When
                val actual = mutableListOf<Map<String, AttributeValue>>()
                val keyExpression = "PK = \"$partitionKey\" and SK between \"SK#1\" and \"SK#50\""
                val where = "IsValid = true"
                val action: (List<Map<String, AttributeValue>>, prev: Map<String, AttributeValue>?, next: Map<String, AttributeValue>?) -> Unit =
                    { list, _, _ ->
                        actual.addAll(list)
                    }
                testee.query(keyExpression = keyExpression, action = action, where = where)

                // Then
                assertThat(actual).isEqualTo(expected)
            }

        @Test
        fun `should return all items in the item collection in reverse order when scanning backwards`(): Unit =
            runBlocking {
                // Given
                val partitionKey = "PK#${ObjectMother.int()}"
                val items = randomItemCollection(partitionKey, 100)
                val expected = items.sortedBy { it["SK"]!!.s() }.reversed().toList()
                val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
                testee.batchWrite(writeRequests)

                // When
                val actual = mutableListOf<Map<String, AttributeValue>>()
                val keyExpression = "PK = \"$partitionKey\" and begins_with(SK, \"SK#\")"
                val action: (List<Map<String, AttributeValue>>, prev: Map<String, AttributeValue>?, next: Map<String, AttributeValue>?) -> Unit =
                    { list, _, _ ->
                        actual.addAll(list)
                    }
                testee.query(keyExpression = keyExpression, action = action, scanForward = false)

                // Then
                assertThat(actual).isEqualTo(expected)
            }

        @Test
        fun `should paginate results`(): Unit = runBlocking {
            // Given
            val partitionKey = "PK#${ObjectMother.int()}"
            val items = randomItemCollection(partitionKey, 100)
            val limit = 10
            val expected = items.sortedBy { it["SK"]!!.s() }.take(limit * 3).toList()
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val keyExpression = "PK = \"$partitionKey\" and begins_with(SK, \"SK#\")"
            var cursor: Map<String, AttributeValue>? = null
            val action: (List<Map<String, AttributeValue>>, prev: Map<String, AttributeValue>?, next: Map<String, AttributeValue>?) -> Unit =
                { list, _, next ->
                    actual.addAll(list)
                    cursor = next!!
                }
            testee.query(keyExpression = keyExpression, action = action, limit = limit)
            testee.query(keyExpression = keyExpression, action = action, limit = limit, start = cursor)
            testee.query(keyExpression = keyExpression, action = action, limit = limit, start = cursor)

            // Then
            assertThat(actual).isEqualTo(expected)
        }

        @Test
        fun `should paginate results going backwards`(): Unit = runBlocking {
            // Given
            val partitionKey = "PK#${ObjectMother.int()}"
            val items = randomItemCollection(partitionKey, 100)
            val limit = 10
            val expected = items.sortedBy { it["SK"]!!.s() }.reversed().take(limit * 3).toList()
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val keyExpression = "PK = \"$partitionKey\" and begins_with(SK, \"SK#\")"
            var cursor: Map<String, AttributeValue>? = null
            val action: (List<Map<String, AttributeValue>>, prev: Map<String, AttributeValue>?, next: Map<String, AttributeValue>?) -> Unit =
                { list, _, next ->
                    actual.addAll(list)
                    cursor = next!!
                }
            testee.query(keyExpression = keyExpression, action = action, scanForward = false, limit = limit)
            testee.query(
                keyExpression = keyExpression,
                action = action,
                scanForward = false,
                start = cursor,
                limit = limit
            )
            testee.query(
                keyExpression = keyExpression,
                action = action,
                scanForward = false,
                start = cursor,
                limit = limit
            )

            // Then
            assertThat(actual).isEqualTo(expected)
        }
    }

    @Nested
    @DisplayName("scan")
    inner class ScanTest {
        @Test
        fun `should return nothing when table is empty`(): Unit = runBlocking {
            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val action: (List<Map<String, AttributeValue>>, Map<String, AttributeValue>?) -> Unit = { list, _ ->
                actual.addAll(list)
            }
            testee.scan(action = action)

            // Then
            assertThat(actual).isEmpty()
        }

        @Test
        fun `should return nothing when GSI is empty`(): Unit = runBlocking {
            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val action: (List<Map<String, AttributeValue>>, Map<String, AttributeValue>?) -> Unit = { list, _ ->
                actual.addAll(list)
            }
            testee.scan(indexName = "GSI1", action = action)

            // Then
            assertThat(actual).isEmpty()
        }

        @Test
        fun `should return all items in the table when no filters are provided`(): Unit = runBlocking {
            // Given
            val items = randomItems(1000)
            val expected = items.toList()
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val action: (List<Map<String, AttributeValue>>, Map<String, AttributeValue>?) -> Unit = { list, _ ->
                actual.addAll(list)
            }
            testee.scan(action = action)

            // Then
            assertThat(actual).hasSameElementsAs(expected)
        }

        @Test
        fun `should return all items in the GSI when no filters are provided`(): Unit = runBlocking {
            // Given
            val items = randomItems(1000)
            val expected = items.toList()
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val action: (List<Map<String, AttributeValue>>, Map<String, AttributeValue>?) -> Unit = { list, _ ->
                actual.addAll(list)
            }
            testee.scan(indexName = "GSI1", action = action)

            // Then
            assertThat(actual).hasSameElementsAs(expected)
        }

        @Test
        fun `should return items filtered by where clause`(): Unit = runBlocking {
            // Given
            val items = randomItems(1000)
            val expected = items.filter { it["IsValid"]!!.bool() == true }
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val action: (List<Map<String, AttributeValue>>, Map<String, AttributeValue>?) -> Unit = { list, _ ->
                actual.addAll(list)
            }
            testee.scan(action = action, where = "IsValid = true")

            // Then
            assertThat(actual).hasSameElementsAs(expected)
        }

        @Test
        fun `should return items filtered by where clause in the GSI`(): Unit = runBlocking {
            // Given
            val items = randomItems(1000)
            val expected = items.filter { it["IsValid"]!!.bool() == true }
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val action: (List<Map<String, AttributeValue>>, Map<String, AttributeValue>?) -> Unit = { list, _ ->
                actual.addAll(list)
            }
            testee.scan(indexName = "GSI1", action = action, where = "IsValid <> false")

            // Then
            assertThat(actual).hasSameElementsAs(expected)
        }

        @Test
        fun `should continue scanning from previous position`(): Unit = runBlocking {
            // Given
            val items = randomItems(1000)
            val limit = 500
            val expected = items.toList()
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            var next: Map<String, AttributeValue>? = null
            val action: (List<Map<String, AttributeValue>>, Map<String, AttributeValue>?) -> Unit = { list, n ->
                actual.addAll(list)
                next = n
            }
            testee.scan(action = action, limit = limit)
            testee.scan(action = action, limit = limit, start = next!!)

            // Then
            assertThat(actual).hasSameElementsAs(expected)
        }

        @Test
        fun `should continue scanning from previous position on GSIs`(): Unit = runBlocking {
            // Given
            val items = randomItems(1000)
            val limit = 500
            val expected = items.toList()
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            var next: Map<String, AttributeValue>? = null
            val action: (List<Map<String, AttributeValue>>, Map<String, AttributeValue>?) -> Unit = { list, n ->
                actual.addAll(list)
                next = n
            }
            testee.scan(indexName = "GSI1", action = action, limit = limit)
            testee.scan(indexName = "GSI1", action = action, limit = limit, start = next!!)

            // Then
            assertThat(actual).hasSameElementsAs(expected)
        }

        @Test
        fun `should limit number of results`(): Unit = runBlocking {
            // Given
            val items = randomItems(1000)
            val limit = 10
            val expected = limit
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val action: (List<Map<String, AttributeValue>>, Map<String, AttributeValue>?) -> Unit = { list, _ ->
                actual.addAll(list)
            }
            testee.scan(action = action, limit = limit)

            // Then
            assertThat(actual).hasSize(expected)
        }

        @Test
        fun `should limit number of results in GSI queries`(): Unit = runBlocking {
            // Given
            val items = randomItems(1000)
            val limit = 10
            val expected = limit
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val action: (List<Map<String, AttributeValue>>, Map<String, AttributeValue>?) -> Unit = { list, _ ->
                actual.addAll(list)
            }
            testee.scan(indexName = "GSI1", action = action, limit = limit)

            // Then
            assertThat(actual).hasSize(expected)
        }
    }

    @Nested
    @DisplayName("parallelScan")
    inner class ParallelScanTest {
        @Test
        fun `should return nothing when table is empty`(): Unit = runBlocking {
            // Given
            val totalSegments = 10
            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val action: (List<Map<String, AttributeValue>>, Int, Map<String, AttributeValue>?) -> Unit = { list, _, _ ->
                actual.addAll(list)
            }
            testee.parallelScan(totalSegments = totalSegments, action = action)

            // Then
            assertThat(actual).isEmpty()
        }

        @Test
        fun `should return nothing when GSI is empty`(): Unit = runBlocking {
            // Given
            val totalSegments = 10
            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val action: (List<Map<String, AttributeValue>>, Int, Map<String, AttributeValue>?) -> Unit = { list, _, _ ->
                actual.addAll(list)
            }
            testee.parallelScan(indexName = "GSI1", totalSegments = totalSegments, action = action)

            // Then
            assertThat(actual).isEmpty()
        }

        @Test
        fun `should return all items in the table when no filters are provided`(): Unit = runBlocking {
            // Given
            val items = randomItems(1000)
            val totalSegments = 10
            val expected = items.toList()
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val action: (List<Map<String, AttributeValue>>, Int, Map<String, AttributeValue>?) -> Unit = { list, _, _ ->
                actual.addAll(list)
            }
            testee.parallelScan(totalSegments = totalSegments, action = action)

            // Then
            assertThat(actual).hasSameElementsAs(expected)
        }

        @Test
        fun `should return all items in the GSI when no filters are provided`(): Unit = runBlocking {
            // Given
            val items = randomItems(1000)
            val totalSegments = 10
            val expected = items.toList()
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val action: (List<Map<String, AttributeValue>>, Int, Map<String, AttributeValue>?) -> Unit = { list, _, _ ->
                actual.addAll(list)
            }
            testee.parallelScan(indexName = "GSI1", totalSegments = totalSegments, action = action)

            // Then
            assertThat(actual).hasSameElementsAs(expected)
        }

        @Test
        fun `should return items filtered by where clause`(): Unit = runBlocking {
            // Given
            val items = randomItems(1000)
            val totalSegments = 10
            val expected = items.filter { it["IsValid"]!!.bool() == true }
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val action: (List<Map<String, AttributeValue>>, Int, Map<String, AttributeValue>?) -> Unit = { list, _, _ ->
                actual.addAll(list)
            }
            testee.parallelScan(totalSegments = totalSegments, action = action, where = "IsValid = true")

            // Then
            assertThat(actual).hasSameElementsAs(expected)
        }

        @Test
        fun `should return items filtered by where clause in the GSI`(): Unit = runBlocking {
            // Given
            val items = randomItems(1000)
            val totalSegments = 10
            val expected = items.filter { it["IsValid"]!!.bool() == true }
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val action: (List<Map<String, AttributeValue>>, Int, Map<String, AttributeValue>?) -> Unit = { list, _, _ ->
                actual.addAll(list)
            }
            testee.parallelScan(
                indexName = "GSI1",
                totalSegments = totalSegments,
                action = action,
                where = "IsValid <> false"
            )

            // Then
            assertThat(actual).hasSameElementsAs(expected)
        }

        @Test
        fun `should continue scanning from previous position`(): Unit = runBlocking {
            // Given
            val items = randomItems(1000)
            val totalSegments = 10
            val limit = 50
            val expected = items.toList()
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val cursors = MutableList<Map<String, AttributeValue>?>(totalSegments) { null }
            val action: (List<Map<String, AttributeValue>>, Int, Map<String, AttributeValue>?) -> Unit =
                { list, index, n ->
                    actual.addAll(list)
                    cursors[index] = n
                }
            testee.parallelScan(totalSegments = totalSegments, action = action, limit = limit)
            while (cursors.any { it != null }) {
                testee.parallelScan(
                    totalSegments = totalSegments,
                    action = action,
                    limit = limit,
                    start = cursors.toList()
                )
            }

            // Then
            assertThat(actual).hasSameElementsAs(expected)
        }

        @Test
        fun `should continue scanning from previous position on GSIs`(): Unit = runBlocking {
            // Given
            val items = randomItems(1000)
            val totalSegments = 10
            val limit = 50
            val expected = items.toList()
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val cursors = MutableList<Map<String, AttributeValue>?>(totalSegments) { null }
            val action: (List<Map<String, AttributeValue>>, Int, Map<String, AttributeValue>?) -> Unit =
                { list, index, n ->
                    actual.addAll(list)
                    cursors[index] = n
                }
            testee.parallelScan(indexName = "GSI1", totalSegments = totalSegments, action = action, limit = limit)
            while (cursors.any { it != null }) {
                testee.parallelScan(
                    indexName = "GSI1",
                    totalSegments = totalSegments,
                    action = action,
                    limit = limit,
                    start = cursors.toList()
                )
            }

            // Then
            assertThat(actual).hasSameElementsAs(expected)
        }

        @Test
        fun `should limit number of results`(): Unit = runBlocking {
            // Given
            val items = randomItems(1000)
            val totalSegments = 2
            val limit = 10
            val expected = totalSegments * limit
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val action: (List<Map<String, AttributeValue>>, Int, Map<String, AttributeValue>?) -> Unit = { list, _, _ ->
                actual.addAll(list)
            }
            testee.parallelScan(totalSegments = totalSegments, action = action, limit = limit)

            // Then
            assertThat(actual).hasSize(expected)
        }

        @Test
        fun `should limit number of results in GSI queries`(): Unit = runBlocking {
            // Given
            val items = randomItems(1000)
            val totalSegments = 2
            val limit = 10
            val expected = totalSegments * limit
            val writeRequests = items.map { item -> WriteRequest.builder().putRequest { it.item(item) }.build() }
            testee.batchWrite(writeRequests)

            // When
            val actual = mutableListOf<Map<String, AttributeValue>>()
            val action: (List<Map<String, AttributeValue>>, Int, Map<String, AttributeValue>?) -> Unit = { list, _, _ ->
                actual.addAll(list)
            }
            testee.parallelScan(indexName = "GSI1", totalSegments = totalSegments, action = action, limit = limit)

            // Then
            assertThat(actual).hasSize(expected)
        }
    }

    private fun randomItems(size: Int? = 100): List<Map<String, AttributeValue>> {
        return (1..size!!).map { i ->
            mapOf(
                "PK" to AttributeValue.builder().s("PK#$i").build(),
                "SK" to AttributeValue.builder().s("SK#$i").build(),
                "Type" to AttributeValue.builder().s("Random").build(),
                "IsValid" to AttributeValue.builder().bool(ObjectMother.coin()).build(),
                "Value" to AttributeValue.builder().n("${ObjectMother.int()}").build(),
                "GSI1PK" to AttributeValue.builder().s("SK#$i").build(),
                "GSI1SK" to AttributeValue.builder().s("PK#$i").build()
            )
        }
    }

    private suspend fun scan(): List<Map<String, AttributeValue>> {
        val items = mutableListOf<Map<String, AttributeValue>>()

        val action: (List<Map<String, AttributeValue>>, Map<String, AttributeValue>?) -> Unit = { list, _ ->
            items.addAll(list)
        }

        testee.scan(action = action)

        return items.toList()
    }
}
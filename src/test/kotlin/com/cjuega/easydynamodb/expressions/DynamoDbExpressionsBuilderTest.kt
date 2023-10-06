package com.cjuega.easydynamodb.expressions

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

class DynamoDbExpressionsBuilderTest {
    @Test
    fun `comparator expression with numbers`() {
        val expression = "property <= 123"

        val expectedExpression = "#property <= :1"
        val expectedNames = mapOf("#property" to "property")
        val expectedValues = mapOf(":1" to AttributeValue.builder().n("123").build())

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `comparator expression with strings`() {
        val expression = "id <> \"abs\""

        val expectedExpression = "#id <> :1"
        val expectedNames = mapOf("#id" to "id")
        val expectedValues = mapOf(":1" to AttributeValue.builder().s("abs").build())

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `comparator expression with booleans`() {
        val expression = "bool = true"

        val expectedExpression = "#bool = :1"
        val expectedNames = mapOf("#bool" to "bool")
        val expectedValues = mapOf(":1" to AttributeValue.builder().bool(true).build())

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `between expression with numbers`() {
        val expression = "id between 1 and 10"

        val expectedExpression = "#id between :1 and :2"
        val expectedNames = mapOf("#id" to "id")
        val expectedValues = mapOf(
            ":1" to AttributeValue.builder().n("1").build(),
            ":2" to AttributeValue.builder().n("10").build()
        )

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `between expression with strings`() {
        val expression = "createdAt between \"2023-09-01\" and \"2023-09-10\""

        val expectedExpression = "#createdAt between :1 and :2"
        val expectedNames = mapOf("#createdAt" to "createdAt")
        val expectedValues = mapOf(
            ":1" to AttributeValue.builder().s("2023-09-01").build(),
            ":2" to AttributeValue.builder().s("2023-09-10").build()
        )

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `in expression with numbers`() {
        val expression = "property in (3, 4, 5)"

        val expectedExpression = "#property in (:1, :2, :3)"
        val expectedNames = mapOf("#property" to "property")
        val expectedValues = mapOf(
            ":1" to AttributeValue.builder().n("3").build(),
            ":2" to AttributeValue.builder().n("4").build(),
            ":3" to AttributeValue.builder().n("5").build()
        )

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `in expression with strings`() {
        val expression = "property in (\"user\", \"admin\")"

        val expectedExpression = "#property in (:1, :2)"
        val expectedNames = mapOf("#property" to "property")
        val expectedValues = mapOf(
            ":1" to AttributeValue.builder().s("user").build(),
            ":2" to AttributeValue.builder().s("admin").build()
        )

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `logical expression (AND) with strings`() {
        val expression = "id = 1 and property = false"

        val expectedExpression = "#id = :1 and #property = :2"
        val expectedNames = mapOf(
            "#id" to "id",
            "#property" to "property"
        )
        val expectedValues = mapOf(
            ":1" to AttributeValue.builder().n("1").build(),
            ":2" to AttributeValue.builder().bool(false).build()
        )

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `logical expression (OR) with strings`() {
        val expression = "date > \"2023-09-01\" or bool = false"

        val expectedExpression = "#date > :1 or #bool = :2"
        val expectedNames = mapOf(
            "#date" to "date",
            "#bool" to "bool"
        )
        val expectedValues = mapOf(
            ":1" to AttributeValue.builder().s("2023-09-01").build(),
            ":2" to AttributeValue.builder().bool(false).build()
        )

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `complex expressions with parenthesis`() {
        val expression = "(id = 1 or inverse <> false) and date > \"2023-09-01\""

        val expectedExpression = "(#id = :1 or #inverse <> :2) and #date > :3"
        val expectedNames = mapOf(
            "#id" to "id",
            "#inverse" to "inverse",
            "#date" to "date"
        )
        val expectedValues = mapOf(
            ":1" to AttributeValue.builder().n("1").build(),
            ":2" to AttributeValue.builder().bool(false).build(),
            ":3" to AttributeValue.builder().s("2023-09-01").build()
        )

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `attribute_exists expression`() {
        val expression = "attribute_exists(id)"

        val expectedExpression = "attribute_exists(#id)"
        val expectedNames = mapOf(
            "#id" to "id"
        )
        val expectedValues = emptyMap<String, AttributeValue>()

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `attribute_exists expression with complex path`() {
        val expression = "attribute_exists(data.property)"

        val expectedExpression = "attribute_exists(#data.#property)"
        val expectedNames = mapOf(
            "#data" to "data",
            "#property" to "property"
        )
        val expectedValues = emptyMap<String, AttributeValue>()

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `attribute_exists expression with spaces`() {
        val expression = "attribute_exists (id)"

        val expectedExpression = "attribute_exists(#id)"
        val expectedNames = mapOf(
            "#id" to "id"
        )
        val expectedValues = emptyMap<String, AttributeValue>()

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `attribute_not_exists expression`() {
        val expression = "attribute_not_exists(id)"

        val expectedExpression = "attribute_not_exists(#id)"
        val expectedNames = mapOf(
            "#id" to "id"
        )
        val expectedValues = emptyMap<String, AttributeValue>()

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `attribute_not_exists expression with complex path`() {
        val expression = "attribute_not_exists(data.property)"

        val expectedExpression = "attribute_not_exists(#data.#property)"
        val expectedNames = mapOf(
            "#data" to "data",
            "#property" to "property"
        )
        val expectedValues = emptyMap<String, AttributeValue>()

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `attribute_not_exists expression with spaces`() {
        val expression = "attribute_not_exists (id)"

        val expectedExpression = "attribute_not_exists(#id)"
        val expectedNames = mapOf(
            "#id" to "id"
        )
        val expectedValues = emptyMap<String, AttributeValue>()

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `begins_with expression`() {
        val expression = "begins_with(PK, \"PURCHASE#123#\")"

        val expectedExpression = "begins_with(#PK, :1)"
        val expectedNames = mapOf(
            "#PK" to "PK"
        )
        val expectedValues = mapOf(
            ":1" to AttributeValue.builder().s("PURCHASE#123#").build()
        )

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `begins_with expression with complex path`() {
        val expression = "begins_with(data.key, \"PURCHASE#123#\")"

        val expectedExpression = "begins_with(#data.#key, :1)"
        val expectedNames = mapOf(
            "#data" to "data",
            "#key" to "key"
        )
        val expectedValues = mapOf(
            ":1" to AttributeValue.builder().s("PURCHASE#123#").build()
        )

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `begins_with expression with spaces`() {
        val expression = "begins_with (PK, \"PURCHASE#123#\")"

        val expectedExpression = "begins_with(#PK, :1)"
        val expectedNames = mapOf(
            "#PK" to "PK"
        )
        val expectedValues = mapOf(
            ":1" to AttributeValue.builder().s("PURCHASE#123#").build()
        )

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `contains expression`() {
        val expression = "contains(PK, \"PURCHASE#123#\")"

        val expectedExpression = "contains(#PK, :1)"
        val expectedNames = mapOf(
            "#PK" to "PK"
        )
        val expectedValues = mapOf(
            ":1" to AttributeValue.builder().s("PURCHASE#123#").build()
        )

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `contains expression with complex path`() {
        val expression = "contains(user.role, \"admin\")"

        val expectedExpression = "contains(#user.#role, :1)"
        val expectedNames = mapOf(
            "#user" to "user",
            "#role" to "role"
        )
        val expectedValues = mapOf(
            ":1" to AttributeValue.builder().s("admin").build()
        )

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `contains expression with spaces`() {
        val expression = "contains (color, 3)"

        val expectedExpression = "contains(#color, :1)"
        val expectedNames = mapOf(
            "#color" to "color"
        )
        val expectedValues = mapOf(
            ":1" to AttributeValue.builder().n("3").build()
        )

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `size expression`() {
        val expression = "size(color) >= 3"

        val expectedExpression = "size(#color) >= :1"
        val expectedNames = mapOf(
            "#color" to "color"
        )
        val expectedValues = mapOf(
            ":1" to AttributeValue.builder().n("3").build()
        )

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `size expression with complex path`() {
        val expression = "size(purchase.statuses) > 0"

        val expectedExpression = "size(#purchase.#statuses) > :1"
        val expectedNames = mapOf(
            "#purchase" to "purchase",
            "#statuses" to "statuses"
        )
        val expectedValues = mapOf(
            ":1" to AttributeValue.builder().n("0").build()
        )

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }

    @Test
    fun `size expression with spaces`() {
        val expression = "size (color) <> 3"

        val expectedExpression = "size(#color) <> :1"
        val expectedNames = mapOf(
            "#color" to "color"
        )
        val expectedValues = mapOf(
            ":1" to AttributeValue.builder().n("3").build()
        )

        val actual = DynamoDbExpressionsBuilder.parseFilterExpression(expression)

        assertNotNull(actual)

        assertThat(actual?.expression).isEqualTo(expectedExpression)
        assertThat(actual?.attributeNames).isEqualTo(expectedNames)
        assertThat(actual?.attributeValues).isEqualTo(expectedValues)
    }
}
package com.cjuega.easydynamodb.expressions

import org.antlr.v4.runtime.tree.TerminalNode
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

class DynamoDbCustomExpressionVisitor(
    attributeNames: Map<String, String>? = mutableMapOf(),
    attributeValues: Map<String, AttributeValue>? = mutableMapOf(),
    index: Int?
) : DynamoDbExpressionBaseVisitor<String>() {
    private val attributeNames = attributeNames?.toMutableMap() ?: mutableMapOf()
    private val attributeValues = attributeValues?.toMutableMap() ?: mutableMapOf()
    private var index = index ?: 1

    fun attributeNames(): Map<String, String> = attributeNames.toMap()

    fun attributeValues(): Map<String, AttributeValue> = attributeValues.toMap()

    fun index(): Int = index

    override fun visitAttributeName(ctx: DynamoDbExpressionParser.AttributeNameContext?): String {
        return ctx?.text?.let {
            val name = "#$it"
            attributeNames[name] = it
            name
        }.orEmpty()
    }

    override fun visitAttributeValue(ctx: DynamoDbExpressionParser.AttributeValueContext?): String {
        return ctx?.text?.let {
            val value = if (it.startsWith("\"") && it.endsWith("\"")) {
                AttributeValue.builder().s(it.substring(1, it.length - 1)).build()
            } else if (it == "true" || it == "false") {
                AttributeValue.builder().bool(it == "true").build()
            } else {
                AttributeValue.builder().n(it).build()
            }

            val valueName = ":$index"
            attributeValues[valueName] = value
            index++
            valueName
        }.orEmpty()
    }

    override fun visitTerminal(node: TerminalNode?): String {
        return node?.symbol?.text.orEmpty()
    }

    override fun aggregateResult(aggregate: String?, nextResult: String?): String {
        if (nextResult == null) {
            return aggregate.orEmpty()
        }

        return if (aggregate != null) {
            if (skipWhiteSpace(aggregate, nextResult)) {
                "$aggregate$nextResult"
            } else {
                "$aggregate $nextResult"
            }
        } else {
            nextResult
        }
    }

    private fun skipWhiteSpace(aggregate: String, nextResult: String): Boolean {
        val isFunction = listOf(
            "attribute_exists",
            "attribute_not_exists",
            "attribute_type",
            "begins_with",
            "contains",
            "size"
        ).any { aggregate.endsWith(it) }
        val noWhiteSpaceStarting = listOf(")", ".", ",").any { nextResult.startsWith(it) }
        val noWhiteSpaceEnding = listOf("(", ".").any { aggregate.endsWith(it) }

        return isFunction || noWhiteSpaceStarting || noWhiteSpaceEnding
    }
}

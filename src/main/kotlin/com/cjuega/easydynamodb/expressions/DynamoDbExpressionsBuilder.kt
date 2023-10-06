package com.cjuega.easydynamodb.expressions

import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

object DynamoDbExpressionsBuilder {
    fun parseKeyExpression(
        expression: String,
        previous: DynamoDbExpression? = null
    ): DynamoDbExpression {
        return parseFilterExpression(expression, previous)
            ?: throw IllegalArgumentException("Expression cannot be null")
    }

    fun parseFilterExpression(expression: String?, previous: DynamoDbExpression? = null): DynamoDbExpression? {
        if (expression.isNullOrEmpty()) {
            return null
        }

        val lexer = DynamoDbExpressionLexer(CharStreams.fromString(expression))
        val tokens = CommonTokenStream(lexer)
        val parser = DynamoDbExpressionParser(tokens)
        val tree = parser.condition()
        val visitor =
            DynamoDbCustomExpressionVisitor(previous?.attributeNames, previous?.attributeValues, previous?.index)

        val mutatedExpression = visitor.visit(tree)

        return DynamoDbExpression(
            mutatedExpression,
            visitor.attributeNames(),
            visitor.attributeValues(),
            visitor.index()
        )
    }

    data class DynamoDbExpression(
        val expression: String,
        val attributeNames: Map<String, String>,
        val attributeValues: Map<String, AttributeValue>,
        val index: Int
    )
}

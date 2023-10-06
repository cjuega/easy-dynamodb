package com.cjuega.easydynamodb

import com.cjuega.easydynamodb.config.DynamoDbConfig
import com.cjuega.easydynamodb.config.DynamoDbPrimaryKey

object TestDynamoDbConfigProvider {
    fun config(): DynamoDbConfig {
        return DynamoDbConfig(
            tableName = "table-test",
            primaryKey = DynamoDbPrimaryKey("PK", "SK"),
            indexes = mapOf("GSI1" to DynamoDbPrimaryKey("GSI1PK", "GSI1SK"))
        )
    }
}
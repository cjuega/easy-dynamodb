package com.cjuega.easydynamodb

import java.net.URI

object DynamoDbContainerProperties {
    fun getDynamoDbProperties(): URI {
        val host = System.getProperty("dynamodb_1.host")
            ?: throw IllegalStateException("DynamoDb host not found. Is docker running?")
        val port = System.getProperty("dynamodb_1.tcp.8000")
            ?: throw IllegalStateException("DynamoDb port not found. Is docker running?")

        return URI("http://$host:$port")
    }
}
package com.example.flinkcommon.core.models

data class KafkaSourceProperties(
    val brokers: List<String>,
    val topic: String,
    val groupId: String,
    val securityProtocol: String = "SASL_PLAINTEXT",
    val saslMechanism: String = "SCRAM-SHA-256",
    val saslJaasConfig: String,
    val autoOffsetReset: OffsetInitializer = OffsetInitializer.LATEST,
    val enableAutoCommit: Boolean = false,
) {
    enum class OffsetInitializer {
        EARLIEST, LATEST
    }
}

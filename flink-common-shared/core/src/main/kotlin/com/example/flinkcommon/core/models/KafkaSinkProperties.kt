package com.example.flinkcommon.core.models

data class KafkaSinkProperties(
    val brokers: List<String>,
    val topic: String,
    val securityProtocol: String = "SASL_PLAINTEXT",
    val saslMechanism: String = "SCRAM-SHA-256",
    val saslJaasConfig: String
)

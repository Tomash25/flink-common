package com.example.flinkcommon.core.bootstrap

import com.example.flinkcommon.core.serialization.factory.SerializationSchemaFactory
import com.example.flinkcommon.core.models.KafkaSinkProperties
import org.apache.flink.api.connector.sink2.Sink
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema

@Connector(configurationPrefix = "kafka")
class KafkaSinkInflator(
    private val serializationSchemaFactory: SerializationSchemaFactory
) : SinkInflator<KafkaSinkProperties> {
    override fun <R> inflateSink(properties: KafkaSinkProperties, clazz: Class<R>): Sink<R> {
        return KafkaSink.builder<R>()
            .setBootstrapServers(properties.brokers.joinToString(","))
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder<R>()
                    .setTopic(properties.topic)
                    .setValueSerializationSchema(this.serializationSchemaFactory.create())
                    .build()
            )
            .setProperty("security.protocol", properties.securityProtocol)
            .setProperty("sasl.mechanism", properties.saslMechanism)
            .setProperty("sasl.jaas.config", properties.saslJaasConfig)
            .setProperty("enable.idempotence", "false")
            .build()
    }
}
package com.example.flinkcommon.core.bootstrap

import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.api.connector.source.Source
import com.example.flinkcommon.core.models.KafkaSourceProperties
import com.example.flinkcommon.core.serialization.factory.DeserializationSchemaFactory

class KafkaSourceInflator(
    private val deserializationSchemaFactory: DeserializationSchemaFactory
) : SourceInflator<KafkaSourceProperties> {
    override fun <R> inflate(
        properties: KafkaSourceProperties,
        clazz: Class<R>
    ): Source<R, *, *> {
        return KafkaSource.builder<R>()
            .setBootstrapServers(properties.brokers.joinToString(","))
            .setGroupId(properties.groupId)
            .setTopics(properties.topic)
            .setStartingOffsets(
                when (properties.autoOffsetReset) {
                    KafkaSourceProperties.OffsetInitializer.EARLIEST -> OffsetsInitializer.earliest()
                    KafkaSourceProperties.OffsetInitializer.LATEST -> OffsetsInitializer.latest()
                }
            )
            .setValueOnlyDeserializer(
                this.deserializationSchemaFactory.create(clazz)
            )
            .setProperty("security.protocol", properties.securityProtocol)
            .setProperty("sasl.mechanism", properties.saslMechanism)
            .setProperty("sasl.jaas.config", properties.saslJaasConfig)
            .setProperty("enable.auto.commit", properties.enableAutoCommit.toString())
            .build()
    }
}

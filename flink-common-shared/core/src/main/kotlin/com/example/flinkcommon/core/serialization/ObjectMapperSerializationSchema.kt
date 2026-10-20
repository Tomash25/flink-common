package com.example.flinkcommon.core.serialization

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import mu.KotlinLogging
import org.apache.flink.api.common.serialization.SerializationSchema
import com.example.flinkcommon.core.serialization.factory.ObjectMapperFactory
import java.io.Serializable

private val log = KotlinLogging.logger { }

class ObjectMapperSerializationSchema<T>(
    private val objectMapperFactory: ObjectMapperFactory,
) : SerializationSchema<T>, Serializable {
    private lateinit var objectMapper: ObjectMapper

    override fun open(context: SerializationSchema.InitializationContext?) {
        this.objectMapper = this.objectMapperFactory.create()
    }

    override fun serialize(record: T): ByteArray? {
        return try {
            this.objectMapper.writeValueAsBytes(record)
        } catch (e: JsonProcessingException) {
            log.error(e) { "Error serializing record" }
            null
        }
    }
}

package com.example.flinkcommon.core.serialization

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import com.example.flinkcommon.core.serialization.factory.ObjectMapperFactory

class ObjectMapperDeserializationSchema<T>(
    private val targetClass: Class<T>,
    private val objectMapperFactory: ObjectMapperFactory
) : DeserializationSchema<T> {
    private lateinit var objectMapper: ObjectMapper

    override fun open(context: DeserializationSchema.InitializationContext?) {
        this.objectMapper = this.objectMapperFactory.create()
    }

    override fun deserialize(p0: ByteArray?): T? = this.objectMapper.readValue(p0, this.targetClass)

    override fun isEndOfStream(p0: T?): Boolean = false

    override fun getProducedType(): TypeInformation<T>? = TypeInformation.of(this.targetClass)
}

package com.example.flinkcommon.core.serialization.factory

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.serialization.SerializationSchema
import com.example.flinkcommon.core.serialization.ObjectMapperDeserializationSchema
import com.example.flinkcommon.core.serialization.ObjectMapperSerializationSchema

class ObjectMapperSerializationSchemaFactory(
    private val objectMapperFactory: ObjectMapperFactory
) : SerializationSchemaFactory, DeserializationSchemaFactory {
    override fun <T> create(): SerializationSchema<T> = ObjectMapperSerializationSchema(objectMapperFactory)

    override fun <T> create(clazz: Class<T>): DeserializationSchema<T> =
        ObjectMapperDeserializationSchema(clazz, objectMapperFactory)
}

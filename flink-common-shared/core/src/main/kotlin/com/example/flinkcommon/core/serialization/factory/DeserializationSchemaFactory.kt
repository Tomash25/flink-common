package com.example.flinkcommon.core.serialization.factory

import org.apache.flink.api.common.serialization.DeserializationSchema

interface DeserializationSchemaFactory {
    fun <T> create(clazz: Class<T>): DeserializationSchema<T>
}

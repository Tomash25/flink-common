package com.example.flinkcommon.core.serialization.factory

import org.apache.flink.api.common.serialization.SerializationSchema

interface SerializationSchemaFactory {
    fun <T> create(): SerializationSchema<T>
}

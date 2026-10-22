package com.example.flinkcommon.core.bootstrap

import org.apache.flink.api.connector.sink2.Sink
import com.example.flinkcommon.core.models.GenericSinkProperties
import kotlin.reflect.full.memberProperties

class GenericSinkInflator(
    private val sinkInflatorRegistry: SinkInflatorRegistry
) {
    fun <R> inflate(properties: GenericSinkProperties, clazz: Class<R>): Sink<R> {
        val firstNonNullValue: Any = properties::class.memberProperties
            .firstNotNullOfOrNull { property -> property.getter.call(properties) }
            ?: throw NullPointerException("No sink was configured")

        return this.inflate(firstNonNullValue, clazz)
    }

    @Suppress("UNCHECKED_CAST")
    private inline fun <reified T, R> inflate(properties: T, recordClass: Class<R>): Sink<R> {
        return (this.sinkInflatorRegistry.retrieve(properties!!::class.java) as SinkInflator<T>)
            .inflate(properties, recordClass)
    }
}
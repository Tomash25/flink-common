package com.example.flinkcommon.core.bootstrap

import com.example.flinkcommon.core.models.GenericSourceProperties
import kotlin.reflect.full.memberProperties
import org.apache.flink.api.connector.source.Source

class GenericSourceInflator(
    private val sourceInflatorRegistry: SourceInflatorRegistry
) {
    fun <R> inflate(properties: GenericSourceProperties, clazz: Class<R>): Source<R, *, *> {
        val firstNonNullValue: Any = properties::class.memberProperties
            .firstNotNullOfOrNull { property -> property.getter.call(properties) }
            ?: throw NullPointerException("No source was configured")

        return this.inflate(firstNonNullValue, clazz)
    }

    @Suppress("UNCHECKED_CAST")
    private inline fun <reified T, R> inflate(properties: T, recordClass: Class<R>): Source<R, *, *> {
        return (this.sourceInflatorRegistry.retrieve(properties!!::class.java) as SourceInflator<T>)
            .inflate(properties, recordClass)
    }
}
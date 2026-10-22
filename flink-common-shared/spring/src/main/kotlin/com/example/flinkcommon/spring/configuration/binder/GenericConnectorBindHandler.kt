package com.example.flinkcommon.spring.configuration.binder

import com.example.flinkcommon.core.bootstrap.GenericSinkInflator
import com.example.flinkcommon.core.bootstrap.GenericSourceInflator
import com.example.flinkcommon.core.models.GenericSinkProperties
import com.example.flinkcommon.core.models.GenericSourceProperties
import org.apache.flink.api.connector.sink2.Sink
import org.apache.flink.api.connector.source.Source
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.context.properties.bind.AbstractBindHandler
import org.springframework.boot.context.properties.bind.BindContext
import org.springframework.boot.context.properties.bind.BindHandler
import org.springframework.boot.context.properties.bind.Bindable
import org.springframework.boot.context.properties.source.ConfigurationPropertyName
import org.springframework.core.ResolvableType
import java.lang.Exception
import java.util.concurrent.ConcurrentHashMap

class GenericConnectorBindHandler(
    private val parentHandler: BindHandler,
    private val sourceInflator: ObjectProvider<GenericSourceInflator>,
    private val sinkInflator: ObjectProvider<GenericSinkInflator>
) : AbstractBindHandler(parentHandler) {
    private val pending = ConcurrentHashMap<ConfigurationPropertyName, Any>()

    @Suppress("UNCHECKED_CAST")
    override fun <T> onStart(
        name: ConfigurationPropertyName,
        target: Bindable<T>,
        context: BindContext
    ): Bindable<T>? {
        val resolved: Class<*> = target.type.resolve()
            ?: return super.onStart(name, target, context)

        if (Source::class.java.isAssignableFrom(resolved)) {
            val recordType: ResolvableType = target.type.getGeneric(0)
            val properties: GenericSourceProperties = context.binder
                .bind(name, Bindable.of(GenericSourceProperties::class.java), this.parentHandler)
                .orElseThrow { IllegalStateException("No configuration found for source under $name") }

            val source: Source<*, *, *> = this.sourceInflator.getObject()
                .inflate(properties, recordType.resolve())

            this.pending[name] = source as Any

            throw BoundConnectorException()
        }

        if (Sink::class.java.isAssignableFrom(resolved)) {
            val recordType: ResolvableType = target.type.getGeneric(0)
            val properties: GenericSinkProperties = context.binder
                .bind(name, Bindable.of(GenericSinkProperties::class.java), this.parentHandler)
                .orElseThrow { IllegalStateException("No configuration found for sink under $name") }

            val sink: Sink<*> = this.sinkInflator.getObject()
                .inflate(properties, recordType.resolve())

            this.pending[name] = sink as Any

            throw BoundConnectorException()
        }

        return super.onStart(name, target, context)
    }

    override fun onFailure(
        name: ConfigurationPropertyName,
        target: Bindable<*>,
        context: BindContext,
        error: Exception
    ): Any? {
        return this.pending.remove(name)
            ?: super.onFailure(name, target, context, error)
    }

    private class BoundConnectorException : RuntimeException()
}
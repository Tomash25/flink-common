package com.example.flinkcommon.spring.configuration.binder

import com.example.flinkcommon.core.bootstrap.Connector
import com.example.flinkcommon.core.bootstrap.SinkInflator
import com.example.flinkcommon.core.bootstrap.SourceInflator
import org.apache.flink.api.connector.sink2.Sink
import org.apache.flink.api.connector.source.Source
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.context.properties.bind.AbstractBindHandler
import org.springframework.boot.context.properties.bind.BindContext
import org.springframework.boot.context.properties.bind.BindHandler
import org.springframework.boot.context.properties.bind.BindResult
import org.springframework.boot.context.properties.bind.Bindable
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.ConfigurationPropertyName
import org.springframework.core.ResolvableType
import org.springframework.core.annotation.AnnotationUtils
import org.springframework.util.ClassUtils

class GenericConnectorBindHandler(
    private val parentHandler: BindHandler,
    private val sourceInflators: ObjectProvider<SourceInflator<*>>,
    private val sinkInflators: ObjectProvider<SinkInflator<*>>
) : AbstractBindHandler(parentHandler) {
    override fun <T> onStart(
        name: ConfigurationPropertyName,
        target: Bindable<T>,
        context: BindContext
    ): Bindable<T>? {
        val resolved: Class<*> = target.type.resolve()
            ?: return super.onStart(name, target, context)

        if (Source::class.java.isAssignableFrom(resolved)) {
            val recordClass: Class<*> = target.type.getGeneric(0).resolve()
                ?: error("Could not resolve record type for source $name")
            throw BoundConnectorException(this.bindSource(name, recordClass, context))
        }

        if (Sink::class.java.isAssignableFrom(resolved)) {
            val recordClass: Class<*> = target.type.getGeneric(0).resolve()
                ?: error("Could not resolve record type for sink $name")
            throw BoundConnectorException(this.bindSink(name, recordClass, context))
        }

        return super.onStart(name, target, context)
    }

    override fun onFailure(
        name: ConfigurationPropertyName,
        target: Bindable<*>,
        context: BindContext,
        error: Exception
    ): Any? {
        return if (error is BoundConnectorException) {
            error.bound
        } else {
            super.onFailure(name, target, context, error)
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun bindSource(
        name: ConfigurationPropertyName,
        recordClass: Class<*>,
        context: BindContext
    ): Source<*, *, *> {
        val binder = Binder(context.sources)
        for (inflator in this.sourceInflators) {
            val connectorPrefix: String = this.resolveConnectorPrefix(inflator)
            val propertiesClass: Class<*> = this.resolvePropertiesType(inflator, SourceInflator::class.java)

            val result: BindResult<*> = binder.bind(
                name.append(connectorPrefix),
                Bindable.of(propertiesClass),
                this.parentHandler
            )

            if (result.isBound) {
                return (inflator as SourceInflator<Any>).inflate(result.get(), recordClass)
            }
        }

        error("No configuration found for source under $name")
    }

    @Suppress("UNCHECKED_CAST")
    private fun bindSink(
        name: ConfigurationPropertyName,
        recordClass: Class<*>,
        context: BindContext
    ): Sink<*> {
        val binder = Binder(context.sources)
        for (inflator in this.sinkInflators) {
            val connectorPrefix: String = this.resolveConnectorPrefix(inflator)
            val propertiesClass: Class<*> = this.resolvePropertiesType(inflator, SinkInflator::class.java)

            val result: BindResult<*> = binder.bind(
                name.append(connectorPrefix),
                Bindable.of(propertiesClass),
                this.parentHandler
            )

            if (result.isBound) {
                return (inflator as SinkInflator<Any>).inflate(result.get(), recordClass)
            }
        }

        error("No configuration found for sink under $name")
    }

    private fun resolveConnectorPrefix(inflator: Any): String {
        val inflatorClass: Class<*> = ClassUtils.getUserClass(inflator.javaClass)
        val connector: Connector = AnnotationUtils.findAnnotation(inflatorClass, Connector::class.java)
            ?: error("Inflator ${inflatorClass.name} must be annotated with @Connector")

        return connector.configurationPrefix
    }

    private fun resolvePropertiesType(inflator: Any, inflatorInterface: Class<*>): Class<*> {
        val inflatorClass: Class<*> = ClassUtils.getUserClass(inflator.javaClass)
        return ResolvableType.forClass(inflatorClass)
            .`as`(inflatorInterface)
            .getGeneric(0)
            .resolve()
            ?: error(
                "Could not resolve properties type for ${inflatorClass.name}. " +
                    "Make sure the inflator implementation declares a concrete generic type argument."
            )
    }

    private class BoundConnectorException(val bound: Any) : RuntimeException() {
        override fun fillInStackTrace(): Throwable = this
    }
}

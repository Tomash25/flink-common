package com.example.flinkcommon.spring.configuration

import com.example.flinkcommon.core.bootstrap.GenericSinkInflator
import com.example.flinkcommon.core.bootstrap.GenericSourceInflator
import com.example.flinkcommon.core.bootstrap.KafkaSinkInflator
import com.example.flinkcommon.core.bootstrap.KafkaSourceInflator
import com.example.flinkcommon.core.bootstrap.SinkInflator
import com.example.flinkcommon.core.bootstrap.SinkInflatorRegistry
import com.example.flinkcommon.core.bootstrap.SourceInflator
import com.example.flinkcommon.core.bootstrap.SourceInflatorRegistry
import com.example.flinkcommon.core.serialization.factory.DeserializationSchemaFactory
import com.example.flinkcommon.core.serialization.factory.SerializationSchemaFactory
import com.example.flinkcommon.spring.configuration.binder.GenericConnectorBindHandler
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.ConfigurationPropertiesBindHandlerAdvisor
import org.springframework.context.annotation.Bean
import org.springframework.core.ResolvableType
import org.springframework.util.ClassUtils

@AutoConfiguration
@AutoConfigureAfter(SerializationSchemaAutoConfiguration::class)
class ConnectorInflatorsAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    fun genericSourceInflator(
        deserializationSchemaFactory: DeserializationSchemaFactory,
        inflators: List<SourceInflator<*>>
    ): GenericSourceInflator {
        val defaultInflators: List<SourceInflator<*>> = listOf(
            KafkaSourceInflator(deserializationSchemaFactory)
        )

        val sourceInflatorRegistry = SourceInflatorRegistry().apply {
            (defaultInflators + inflators).forEach { registerTyped(it) }
        }

        return GenericSourceInflator(sourceInflatorRegistry)
    }

    @Bean
    @ConditionalOnMissingBean
    fun genericSinkInflator(
        serializationSchemaFactory: SerializationSchemaFactory,
        inflators: List<SinkInflator<*>>
    ): GenericSinkInflator {
        val defaultInflators: List<SinkInflator<*>> = listOf(
            KafkaSinkInflator(serializationSchemaFactory)
        )

        val sinkInflatorRegistry = SinkInflatorRegistry().apply {
            (defaultInflators + inflators).forEach { registerTyped(it) }
        }

        return GenericSinkInflator(sinkInflatorRegistry)
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> SourceInflatorRegistry.registerTyped(inflator: SourceInflator<T>) {
        this.register(inflator, resolvePropertiesType(inflator) as Class<T>)
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> SinkInflatorRegistry.registerTyped(inflator: SinkInflator<T>) {
        this.register(inflator, resolvePropertiesType(inflator) as Class<T>)
    }

    @Bean
    @ConditionalOnMissingBean
    fun genericConnectorBindAdvisor(
        sourceInflator: ObjectProvider<GenericSourceInflator>,
        sinkInflator: ObjectProvider<GenericSinkInflator>
    ): ConfigurationPropertiesBindHandlerAdvisor {
        return ConfigurationPropertiesBindHandlerAdvisor { parent ->
            GenericConnectorBindHandler(parent, sourceInflator, sinkInflator)
        }
    }

    private fun resolvePropertiesType(inflator: SourceInflator<*>): Class<*> {
        val inflatorClass: Class<*> = ClassUtils.getUserClass(inflator.javaClass)
        val propertiesType = ResolvableType.forClass(inflatorClass)
            .`as`(SourceInflator::class.java)
            .getGeneric(0)

        return propertiesType.resolve()
            ?: throw IllegalStateException(
                "Could not resolve source properties type for ${inflatorClass.name}. " +
                        "Make sure the SourceInflator implementation declares a concrete generic type argument."
            )
    }

    private fun resolvePropertiesType(inflator: SinkInflator<*>): Class<*> {
        val inflatorClass: Class<*> = ClassUtils.getUserClass(inflator.javaClass)
        val propertiesType = ResolvableType.forClass(inflatorClass)
            .`as`(SinkInflator::class.java)
            .getGeneric(0)

        return propertiesType.resolve()
            ?: throw IllegalStateException(
                "Could not resolve sink properties type for ${inflatorClass.name}. " +
                    "Make sure the SinkInflator implementation declares a concrete generic type argument."
            )
    }
}

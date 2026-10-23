package com.example.flinkcommon.spring.configuration

import com.example.flinkcommon.core.serialization.factory.ObjectMapperFactory
import com.example.flinkcommon.core.serialization.factory.ObjectMapperSerializationSchemaFactory
import com.example.flinkcommon.spring.factory.JacksonPropertiesObjectMapperFactory
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.jackson.JacksonProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean

@AutoConfiguration
@EnableConfigurationProperties(JacksonProperties::class)
class SerializationSchemaAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    fun jacksonPropertiesObjectMapperFactory(jacksonProperties: JacksonProperties): ObjectMapperFactory =
        JacksonPropertiesObjectMapperFactory(jacksonProperties)

    @Bean
    @ConditionalOnMissingBean
    fun objectMapperSerializationSchemaFactory(
        objectMapperFactory: ObjectMapperFactory
    ): ObjectMapperSerializationSchemaFactory {
        return ObjectMapperSerializationSchemaFactory(objectMapperFactory)
    }
}

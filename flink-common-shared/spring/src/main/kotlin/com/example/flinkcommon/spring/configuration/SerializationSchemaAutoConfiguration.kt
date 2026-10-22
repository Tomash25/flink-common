package com.example.flinkcommon.spring.configuration

import com.example.flinkcommon.core.serialization.ObjectMapperDeserializationSchema
import com.example.flinkcommon.core.serialization.ObjectMapperSerializationSchema
import com.example.flinkcommon.core.serialization.factory.ObjectMapperFactory
import com.example.flinkcommon.core.serialization.factory.ObjectMapperSerializationSchemaFactory
import com.example.flinkcommon.spring.factory.JacksonPropertiesObjectMapperFactory
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.jackson.JacksonProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean

@AutoConfiguration
@ConditionalOnClass(ObjectMapperDeserializationSchema::class, ObjectMapperSerializationSchema::class)
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

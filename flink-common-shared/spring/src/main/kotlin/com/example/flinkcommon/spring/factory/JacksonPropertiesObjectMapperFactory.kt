package com.example.flinkcommon.spring.factory

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.example.flinkcommon.core.serialization.factory.ObjectMapperFactory
import org.springframework.boot.autoconfigure.jackson.JacksonProperties
import java.util.TimeZone
import java.util.Locale
import java.text.SimpleDateFormat

class JacksonPropertiesObjectMapperFactory(
    jacksonProperties: JacksonProperties
) : ObjectMapperFactory {
    private val deserializationFeatures: Map<DeserializationFeature?, Boolean?> = jacksonProperties.deserialization
    private val serializationFeatures: Map<SerializationFeature?, Boolean?> = jacksonProperties.serialization
    private val jsonParserFeatures: Map<JsonParser.Feature?, Boolean?> = jacksonProperties.parser
    private val defaultPropertyInclusion: JsonInclude.Include? = jacksonProperties.defaultPropertyInclusion
    private val timezone: TimeZone? = jacksonProperties.timeZone
    private val dateFormat: String? = jacksonProperties.dateFormat
    private val locale: Locale? = jacksonProperties.locale

    override fun create(): ObjectMapper {
        return ObjectMapper()
            .setSerializationInclusion(this.defaultPropertyInclusion)
            .setTimeZone(this.timezone)
            .setDateFormat(this.dateFormat?.let { SimpleDateFormat(it) })
            .setLocale(this.locale)
            .registerModule(Jdk8Module())
            .also { mapper ->
                this.serializationFeatures.forEach { (feature, enabled) -> mapper.configure(feature, enabled ?: false) }
                this.deserializationFeatures.forEach { (feature, enabled) -> 
                    mapper.configure(feature, enabled ?: false)
                }
                this.jsonParserFeatures.forEach { (feature, enabled) -> mapper.configure(feature, enabled ?: false) }
            }
    }
}
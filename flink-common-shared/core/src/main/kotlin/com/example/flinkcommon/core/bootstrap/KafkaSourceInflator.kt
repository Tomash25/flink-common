package com.example.flinkcommon.core.bootstrap

class KafkaSourceInflator(
    private val deserializationSchemaFactory: DeserializationSchemaFactory
) : SourceInflator<KafkaSourceProperties> {
    override fun <R> inflate(
        properties: KafkaSourceProperties,
        clazz: Class<R>
    ): Source<R, *, *> {
        return KafkaSource.builder<R>()
    }
}
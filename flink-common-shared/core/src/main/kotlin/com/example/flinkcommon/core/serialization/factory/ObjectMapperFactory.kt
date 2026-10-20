package com.example.flinkcommon.core.serialization.factory

import com.fasterxml.jackson.databind.ObjectMapper
import java.io.Serializable

fun interface ObjectMapperFactory : Serializable {
    fun create(): ObjectMapper
}

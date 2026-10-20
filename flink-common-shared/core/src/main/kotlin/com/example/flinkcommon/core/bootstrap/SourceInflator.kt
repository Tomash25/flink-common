package com.example.flinkcommon.core.bootstrap

import org.apache.flink.api.connector.source.Source

interface SourceInflator<in T> {
    fun <R> inflate(properties: T, clazz: Class<R>): Source<R, *, *>
}

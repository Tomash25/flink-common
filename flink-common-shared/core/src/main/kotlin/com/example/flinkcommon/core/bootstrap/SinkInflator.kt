package com.example.flinkcommon.core.bootstrap

import org.apache.flink.api.connector.sink2.Sink

interface SinkInflator<in T> {
    fun <R> inflateSink(properties: T, clazz: Class<R>): Sink<R>
}

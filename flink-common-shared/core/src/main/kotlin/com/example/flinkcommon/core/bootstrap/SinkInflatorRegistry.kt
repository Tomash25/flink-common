package com.example.flinkcommon.core.bootstrap

class SinkInflatorRegistry {
    private val inflators = mutableMapOf<Class<*>, SinkInflator<*>>()

    fun <T> register(inflator: SinkInflator<T>, clazz: Class<T>) {
        this.inflators[clazz] = inflator
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> retrieve(clazz: Class<T>): SinkInflator<T> = this.inflators[clazz] as? SinkInflator<T>
        ?: throw NullPointerException("Couldn't find a sink inflator for class $clazz")
}
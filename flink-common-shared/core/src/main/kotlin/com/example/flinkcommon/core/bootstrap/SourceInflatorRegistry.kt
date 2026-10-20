package com.example.flinkcommon.core.bootstrap

class SourceInflatorRegistry {
    private val inflators = mutableMapOf<Class<*>, SourceInflator<*>>()

    fun <T> register(inflator: SourceInflator<T>, clazz: Class<T>) {
        this.inflators[clazz] = inflator
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> retrieve(clazz: Class<T>): SourceInflator<T> = inflators[clazz] as? SourceInflator<T>
        ?: throw NullPointerException("Couldn't find a source inflator for class $clazz")
}
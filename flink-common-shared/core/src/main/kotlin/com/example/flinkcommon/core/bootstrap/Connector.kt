package com.example.flinkcommon.core.bootstrap

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
annotation class Connector(val configurationPrefix: String)

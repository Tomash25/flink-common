package com.example.flinkcommon.spring.configuration.properties

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.NestedConfigurationProperty
import com.example.flinkcommon.core.models.StreamExecutionEnvironmentProperties

@ConfigurationProperties(prefix = "spring.flink")
class FlinkProperties(
    @NestedConfigurationProperty
    val streamExecutionEnvironmentProperties: StreamExecutionEnvironmentProperties
)

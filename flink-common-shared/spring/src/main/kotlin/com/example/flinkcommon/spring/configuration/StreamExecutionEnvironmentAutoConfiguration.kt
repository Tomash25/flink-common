package com.example.flinkcommon.spring.configuration

import com.example.flinkcommon.core.bootstrap.StreamExecutionEnvironmentBootstrapper
import com.example.flinkcommon.spring.configuration.properties.FlinkProperties
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean

@AutoConfiguration
@EnableConfigurationProperties(FlinkProperties::class)
class StreamExecutionEnvironmentAutoConfiguration(
    private val properties: FlinkProperties
) {
    @Bean
    @ConditionalOnMissingBean
    fun streamExecutionEnvironmentBootstrapper(): StreamExecutionEnvironmentBootstrapper =
        StreamExecutionEnvironmentBootstrapper()
    
    @Bean
    @ConditionalOnMissingBean
    fun streamExecutionEnvironment(bootstrapper: StreamExecutionEnvironmentBootstrapper): StreamExecutionEnvironment {
        return StreamExecutionEnvironment.getExecutionEnvironment().also { 
            bootstrapper.bootstrap(
                it, 
                this.properties.streamExecutionEnvironmentProperties
            )
         }
    }
}

package com.example.flinkcommon.spring.configuration

import com.example.flinkcommon.core.bootstrap.StreamExecutionEnvironmentBootstrapper
import com.example.flinkcommon.spring.configuration.properties.FlinkProperties
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

@AutoConfiguration
@ConditionalOnClass(StreamExecutionEnvironmentBootstrapper::class)
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
    fun streamExecutionEnvironemnt(bootstrapper: StreamExecutionEnvironmentBootstrapper): StreamExecutionEnvironment {
        return StreamExecutionEnvironment.getExecutionEnvironment().also { 
            bootstrapper.bootstrap(
                it, 
                this.properties.streamExecutionEnvironmentProperties
            )
         }
    }
}

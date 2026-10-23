package com.example.flinkcommon.spring.configuration

import com.example.flinkcommon.core.bootstrap.SinkInflator
import com.example.flinkcommon.core.bootstrap.SourceInflator
import com.example.flinkcommon.spring.configuration.binder.GenericConnectorBindHandler
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.ConfigurationPropertiesBindHandlerAdvisor
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import

@AutoConfiguration
@Import(ConnectorInflatorsRegistrar::class)
class ConnectorInflatorsAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    fun genericConnectorBindAdvisor(
        sourceInflators: ObjectProvider<SourceInflator<*>>,
        sinkInflators: ObjectProvider<SinkInflator<*>>
    ): ConfigurationPropertiesBindHandlerAdvisor {
        return ConfigurationPropertiesBindHandlerAdvisor { parent ->
            GenericConnectorBindHandler(parent, sourceInflators, sinkInflators)
        }
    }
}

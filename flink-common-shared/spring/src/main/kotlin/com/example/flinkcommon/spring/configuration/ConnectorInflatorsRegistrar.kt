package com.example.flinkcommon.spring.configuration

import com.example.flinkcommon.core.bootstrap.Connector
import org.springframework.beans.factory.BeanFactory
import org.springframework.beans.factory.BeanFactoryAware
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition
import org.springframework.beans.factory.config.BeanDefinition
import org.springframework.beans.factory.support.BeanDefinitionRegistry
import org.springframework.boot.autoconfigure.AutoConfigurationPackages
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar
import org.springframework.core.type.AnnotationMetadata
import org.springframework.core.type.filter.AnnotationTypeFilter

class ConnectorInflatorsRegistrar : ImportBeanDefinitionRegistrar, BeanFactoryAware {
    private lateinit var beanFactory: BeanFactory
    private val scanner = ClassPathScanningCandidateComponentProvider(false).apply {
        this.addIncludeFilter(AnnotationTypeFilter(Connector::class.java))
    }

    override fun setBeanFactory(beanFactory: BeanFactory) {
        this.beanFactory = beanFactory
    }

    override fun registerBeanDefinitions(
        importingClassMetadata: AnnotationMetadata,
        registry: BeanDefinitionRegistry
    ) {
        val packages = mutableSetOf(DEFAULT_CONNECTOR_PACKAGE)

        if (AutoConfigurationPackages.has(this.beanFactory)) {
            packages.addAll(AutoConfigurationPackages.get(this.beanFactory))
        }

        packages.forEach { this.registerCandidatesInPackage(registry, it) }
    }

    private fun registerCandidatesInPackage(
        registry: BeanDefinitionRegistry,
        basePackage: String
    ) {
        for (candidate in this.scanner.findCandidateComponents(basePackage)) {
            val beanClassName: String = candidate.beanClassName ?: continue
            if (!registry.hasBeanOfClass(beanClassName)) {
                registry.registerBeanDefinition(beanClassName, candidate)
            }
        }
    }

    private fun BeanDefinitionRegistry.hasBeanOfClass(className: String): Boolean =
        this.beanDefinitionNames.any { this.getBeanDefinition(it).isOfClass(className) }

    private fun BeanDefinition.isOfClass(className: String): Boolean {
        return this.beanClassName == className ||
                (this as? AnnotatedBeanDefinition)?.factoryMethodMetadata?.returnTypeName == className
    }

    companion object {
        private const val DEFAULT_CONNECTOR_PACKAGE = "com.example.flinkcommon"
    }
}

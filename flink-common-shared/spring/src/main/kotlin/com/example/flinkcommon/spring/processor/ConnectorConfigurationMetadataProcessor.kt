package com.example.flinkcommon.spring.processor

import com.example.flinkcommon.core.bootstrap.Connector
import com.example.flinkcommon.spring.processor.ConfigurationMetadataStore.MetadataGroup
import com.example.flinkcommon.spring.processor.ConfigurationMetadataStore.MetadataProperty
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import org.springframework.core.type.filter.AnnotationTypeFilter
import javax.annotation.processing.AbstractProcessor
import javax.annotation.processing.RoundEnvironment
import javax.annotation.processing.SupportedAnnotationTypes
import javax.lang.model.SourceVersion
import javax.lang.model.element.TypeElement
import javax.lang.model.type.DeclaredType
import javax.lang.model.type.TypeMirror

@SupportedAnnotationTypes("*")
class ConnectorConfigurationMetadataProcessor : AbstractProcessor() {
    private val groups = mutableMapOf<String, MetadataGroup>()
    private val properties = mutableMapOf<String, MetadataProperty>()
    private val classpathScanner = ClassPathScanningCandidateComponentProvider(false).apply {
        this.addIncludeFilter(AnnotationTypeFilter(Connector::class.java))
        this.resourceLoader = PathMatchingResourcePatternResolver(
            ConnectorConfigurationMetadataProcessor::class.java.classLoader
        )
    }

    override fun process(annotations: Set<TypeElement>, roundEnv: RoundEnvironment): Boolean {
        if (!roundEnv.processingOver()) {
            val connectors = this.discoverConnectors(roundEnv)
            this.processConfigurationProperties(roundEnv, connectors)

            ConfigurationMetadataStore.writeAdditionalMetadata(
                processingEnv,
                this.groups.values,
                this.properties.values
            )
        }

        return false
    }

    private fun discoverConnectors(roundEnv: RoundEnvironment): List<ConnectorDescriptor> {
        val classpathConnectors: List<TypeElement> = this.classpathScanner.findCandidateComponents(BASE_SCAN_PACKAGE)
            .mapNotNull { this.processingEnv.elementUtils.getTypeElement(it.beanClassName) }

        val sourceConnectors: List<TypeElement> = roundEnv.getElementsAnnotatedWith(Connector::class.java)
            .filterIsInstance<TypeElement>()

        return (classpathConnectors + sourceConnectors)
            .distinctBy { it.qualifiedName }
            .flatMap { this.buildConnectorDescriptors(it) }
    }

    private fun processConfigurationProperties(roundEnv: RoundEnvironment, connectors: List<ConnectorDescriptor>) {
        val annotationType: TypeElement = this.processingEnv.elementUtils.getTypeElement(CONFIG_PROPS_ANNOTATION)
            ?: return

        roundEnv.getElementsAnnotatedWith(annotationType)
            .filterIsInstance<TypeElement>()
            .forEach { element ->
                val prefix: String = MetadataElementUtils.camelToKebab(
                    MetadataElementUtils.extractAnnotationStringValue(
                        element,
                        CONFIG_PROPS_ANNOTATION,
                        listOf("prefix", "value")
                    ).orEmpty()
                )

                this.extractPropertiesFromType(prefix, element, connectors)
            }
    }

    private fun buildConnectorDescriptors(element: TypeElement): List<ConnectorDescriptor> {
        val connectorPrefix: String = MetadataElementUtils.camelToKebab(
            MetadataElementUtils.extractAnnotationStringValue(
                element,
                CONNECTOR_ANNOTATION,
                listOf("configurationPrefix")
            ) ?: return emptyList()
        )

        val inflatorBinaryName: String = this.processingEnv.elementUtils.getBinaryName(element).toString()
        val supertypeGenerics: Map<String, List<TypeElement>> = MetadataElementUtils.extractSupertypeGenericArguments(
            this.processingEnv,
            element.asType()
        )

        val descriptors = mutableListOf<ConnectorDescriptor>()

        supertypeGenerics[SOURCE_INFLATOR_INTERFACE]?.let {
            descriptors += ConnectorDescriptor(
                connectorPrefix,
                inflatorBinaryName,
                it.first(),
                ConnectorDescriptor.Type.SOURCE
            )
        }

        supertypeGenerics[SINK_INFLATOR_INTERFACE]?.let {
            descriptors += ConnectorDescriptor(
                connectorPrefix,
                inflatorBinaryName,
                it.first(),
                ConnectorDescriptor.Type.SINK
            )
        }

        return descriptors
    }

    private fun addConnectorMetadata(
        propertyPrefix: String,
        fieldType: TypeMirror,
        ownerBinaryName: String,
        matchingConnectors: List<ConnectorDescriptor>
    ) {
        this.properties[propertyPrefix] = MetadataProperty(
            name = propertyPrefix,
            type = MetadataElementUtils.formatPropertyType(this.processingEnv, fieldType),
            sourceType = ownerBinaryName
        )

        for (connector in matchingConnectors) {
            val connectorPrefix = "$propertyPrefix.${connector.connectorPrefix}"
            val propsBinaryName = this.processingEnv.elementUtils.getBinaryName(connector.propertiesType).toString()
            this.groups[connectorPrefix] = MetadataGroup(
                name = connectorPrefix,
                type = propsBinaryName,
                sourceType = connector.inflatorTypeName
            )

            this.extractPropertiesFromType(connectorPrefix, connector.propertiesType, matchingConnectors)
        }
    }

    private fun extractPropertiesFromType(
        prefix: String,
        typeElement: TypeElement,
        connectors: List<ConnectorDescriptor>,
        visited: Set<String> = emptySet()
    ) {
        val qualifiedName: String = typeElement.qualifiedName.toString()
        if (qualifiedName in visited) {
            return
        }

        val ownerBinaryName: String = this.processingEnv.elementUtils.getBinaryName(typeElement).toString()
        val members: Map<String, Pair<TypeMirror, Boolean>> = MetadataElementUtils.extractMemberProperties(typeElement)

        for ((memberName, memberInfo) in members) {
            val (memberType, isNested) = memberInfo
            val kebabMember: String = MetadataElementUtils.camelToKebab(memberName)
            val propName: String = if (prefix.isEmpty()) kebabMember else "$prefix.$kebabMember"
            val rawTypeName: String? = ((memberType as? DeclaredType)?.asElement() as? TypeElement)
                ?.qualifiedName?.toString()
            val nestedClass: TypeElement? = if (isNested) {
                (memberType as? DeclaredType)?.asElement() as? TypeElement
            } else {
                MetadataElementUtils.resolveNestedConfigClass(memberType)
            }

            if (rawTypeName == FLINK_SOURCE_INTERFACE) {
                this.addConnectorMetadata(
                    propName,
                    memberType,
                    ownerBinaryName,
                    connectors.filter { it.type == ConnectorDescriptor.Type.SOURCE }
                )
            } else if (rawTypeName == FLINK_SINK_INTERFACE) {
                this.addConnectorMetadata(
                    propName,
                    memberType,
                    ownerBinaryName,
                    connectors.filter { it.type == ConnectorDescriptor.Type.SINK }
                )
            } else if (nestedClass != null) {
                val nestedBinaryName: String = this.processingEnv.elementUtils.getBinaryName(nestedClass).toString()
                this.groups[propName] = MetadataGroup(
                    name = propName,
                    type = nestedBinaryName,
                    sourceType = ownerBinaryName
                )
                this.extractPropertiesFromType(propName, nestedClass, connectors, visited + qualifiedName)
            } else {
                this.properties[propName] = MetadataProperty(
                    name = propName,
                    type = MetadataElementUtils.formatPropertyType(this.processingEnv, memberType),
                    sourceType = ownerBinaryName
                )
            }
        }
    }

    override fun getSupportedSourceVersion(): SourceVersion = SourceVersion.latestSupported()

    private data class ConnectorDescriptor(
        val connectorPrefix: String,
        val inflatorTypeName: String,
        val propertiesType: TypeElement,
        val type: Type
    ) {
        enum class Type {
            SOURCE,
            SINK
        }
    }

    companion object {
        private const val BASE_SCAN_PACKAGE = "com.example"
        private const val CONNECTOR_ANNOTATION = "com.example.flinkcommon.core.bootstrap.Connector"
        private const val SOURCE_INFLATOR_INTERFACE = "com.example.flinkcommon.core.bootstrap.SourceInflator"
        private const val SINK_INFLATOR_INTERFACE = "com.example.flinkcommon.core.bootstrap.SinkInflator"
        private const val FLINK_SOURCE_INTERFACE = "org.apache.flink.api.connector.source.Source"
        private const val FLINK_SINK_INTERFACE = "org.apache.flink.api.connector.sink2.Sink"
        private const val CONFIG_PROPS_ANNOTATION =
            "org.springframework.boot.context.properties.ConfigurationProperties"
    }
}

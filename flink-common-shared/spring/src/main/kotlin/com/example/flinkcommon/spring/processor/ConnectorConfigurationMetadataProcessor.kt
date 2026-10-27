package com.example.flinkcommon.spring.processor

import com.example.flinkcommon.core.bootstrap.Connector
import com.example.flinkcommon.core.bootstrap.SinkInflator
import com.example.flinkcommon.core.bootstrap.SourceInflator
import com.example.flinkcommon.spring.processor.MetadataElementUtils.qualifiedName
import org.springframework.boot.configurationprocessor.metadata.ConfigurationMetadata
import org.springframework.boot.configurationprocessor.metadata.ItemMetadata
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import org.springframework.core.type.filter.AnnotationTypeFilter
import javax.annotation.processing.AbstractProcessor
import javax.annotation.processing.RoundEnvironment
import javax.annotation.processing.SupportedAnnotationTypes
import javax.lang.model.SourceVersion
import javax.lang.model.element.TypeElement
import javax.lang.model.type.TypeMirror

@SupportedAnnotationTypes("*")
class ConnectorConfigurationMetadataProcessor : AbstractProcessor() {
    private val metadata = ConfigurationMetadata()
    private val classpathScanner = ClassPathScanningCandidateComponentProvider(false).apply {
        this.addIncludeFilter(AnnotationTypeFilter(Connector::class.java))
        this.resourceLoader = PathMatchingResourcePatternResolver(
            ConnectorConfigurationMetadataProcessor::class.java.classLoader
        )
    }

    override fun process(annotations: Set<TypeElement>, roundEnv: RoundEnvironment): Boolean {
        if (!roundEnv.processingOver()) {
            val connectors: List<ConnectorDescriptor> = this.discoverConnectors(roundEnv)
            this.processConfigurationProperties(roundEnv, connectors)
            ConfigurationMetadataStore.writeAdditionalMetadata(this.processingEnv, this.metadata)
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
        roundEnv.getElementsAnnotatedWith(ConfigurationProperties::class.java)
            .filterIsInstance<TypeElement>()
            .forEach { element ->
                val configProps: ConfigurationProperties = element.getAnnotation(ConfigurationProperties::class.java)
                    ?: return@forEach

                val rawPrefix: String = configProps.prefix.ifEmpty { configProps.value }
                val prefix: String = ConfigurationMetadata.nestedPrefix(null, rawPrefix)

                this.extractPropertiesFromType(prefix, element, connectors)
            }
    }

    private fun buildConnectorDescriptors(element: TypeElement): List<ConnectorDescriptor> {
        val connectorPrefix: String = element.getAnnotation(Connector::class.java)
            ?.configurationPrefix
            ?.takeIf { it.isNotEmpty() }
            ?.let { ConfigurationMetadata.nestedPrefix(null, it) }
            ?: return emptyList()

        val supertypeGenerics: Map<String, List<TypeElement>> = MetadataElementUtils.extractSupertypeGenericArguments(
            this.processingEnv,
            element.asType()
        )

        return ConnectorDescriptor.Type.entries.mapNotNull { type ->
            supertypeGenerics[type.inflatorClass.name]?.firstOrNull()?.let { propertiesType ->
                ConnectorDescriptor(
                    connectorPrefix = connectorPrefix,
                    inflatorTypeName = element.binaryName(),
                    propertiesType = propertiesType,
                    type = type
                )
            }
        }
    }

    private fun extractPropertiesFromType(
        prefix: String,
        typeElement: TypeElement,
        connectors: List<ConnectorDescriptor>
    ) {
        val ownerBinaryName: String = typeElement.binaryName()
        val members: Map<String, TypeMirror> = MetadataElementUtils.extractMemberProperties(typeElement)

        for ((memberName, memberType) in members) {
            val propertyName: String = ConfigurationMetadata.nestedPrefix(prefix, memberName)
            val rawTypeName: String? = memberType.qualifiedName()
            val nestedClass: TypeElement? = MetadataElementUtils.resolveNestedConfigClass(memberType)
            val connectorType: ConnectorDescriptor.Type? = ConnectorDescriptor.Type.entries.find {
                it.flinkInterface == rawTypeName
            }

            if (connectorType != null) {
                this.addConnectorMetadata(
                    propertyName,
                    memberType,
                    ownerBinaryName,
                    connectors.filter { it.type == connectorType }
                )
            } else if (nestedClass != null) {
                this.metadata.addIfMissing(
                    ItemMetadata.newGroup(propertyName, nestedClass.binaryName(), ownerBinaryName, null)
                )

                this.extractPropertiesFromType(propertyName, nestedClass, connectors)
            } else {
                this.metadata.addIfMissing(
                    ItemMetadata.newProperty(
                        propertyName,
                        null,
                        MetadataElementUtils.formatPropertyType(this.processingEnv, memberType),
                        ownerBinaryName,
                        null,
                        null,
                        null,
                        null
                    )
                )
            }
        }
    }

    private fun addConnectorMetadata(
        propertyPrefix: String,
        fieldType: TypeMirror,
        ownerBinaryName: String,
        matchingConnectors: List<ConnectorDescriptor>
    ) {
        this.metadata.addIfMissing(
            ItemMetadata.newProperty(
                propertyPrefix,
                null,
                MetadataElementUtils.formatPropertyType(this.processingEnv, fieldType),
                ownerBinaryName,
                null,
                null,
                null,
                null
            )
        )

        for (connector in matchingConnectors) {
            val connectorPrefix: String = ConfigurationMetadata.nestedPrefix(propertyPrefix, connector.connectorPrefix)

            this.metadata.addIfMissing(
                ItemMetadata.newGroup(
                    connectorPrefix,
                    connector.propertiesType.binaryName(),
                    connector.inflatorTypeName,
                    null
                )
            )

            this.extractPropertiesFromType(connectorPrefix, connector.propertiesType, matchingConnectors)
        }
    }

    private fun TypeElement.binaryName(): String =
        this@ConnectorConfigurationMetadataProcessor.processingEnv.elementUtils.getBinaryName(this).toString()

    override fun getSupportedSourceVersion(): SourceVersion = SourceVersion.latestSupported()

    private data class ConnectorDescriptor(
        val connectorPrefix: String,
        val inflatorTypeName: String,
        val propertiesType: TypeElement,
        val type: Type
    ) {
        enum class Type(
            val inflatorClass: Class<*>,
            val flinkInterface: String
        ) {
            SOURCE(SourceInflator::class.java, FLINK_SOURCE_INTERFACE),
            SINK(SinkInflator::class.java, FLINK_SINK_INTERFACE)
        }
    }

    companion object {
        private const val BASE_SCAN_PACKAGE = "com.example"
        private const val FLINK_SOURCE_INTERFACE = "org.apache.flink.api.connector.source.Source"
        private const val FLINK_SINK_INTERFACE = "org.apache.flink.api.connector.sink2.Sink"
    }
}

package com.example.flinkcommon.spring.processor

import com.example.flinkcommon.core.bootstrap.Connector
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider
import org.springframework.core.env.StandardEnvironment
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import org.springframework.core.type.filter.AnnotationTypeFilter
import javax.annotation.processing.AbstractProcessor
import javax.annotation.processing.ProcessingEnvironment
import javax.annotation.processing.RoundEnvironment
import javax.annotation.processing.SupportedAnnotationTypes
import javax.lang.model.SourceVersion
import javax.lang.model.element.Element
import javax.lang.model.element.ElementKind
import javax.lang.model.element.ExecutableElement
import javax.lang.model.element.Modifier
import javax.lang.model.element.TypeElement
import javax.lang.model.element.VariableElement
import javax.lang.model.type.DeclaredType
import javax.lang.model.type.PrimitiveType
import javax.lang.model.type.TypeMirror
import javax.lang.model.type.WildcardType

internal data class ConnectorDescriptor(
    val connectorPrefix: String,
    val inflatorTypeName: String,
    val propertiesType: TypeElement,
    val isSource: Boolean
)

@SupportedAnnotationTypes("*")
class ConnectorConfigurationMetadataProcessor : AbstractProcessor() {
    private val groups = mutableMapOf<String, MetadataGroup>()
    private val properties = mutableMapOf<String, MetadataProperty>()
    private val localConnectors = mutableSetOf<String>()

    override fun getSupportedSourceVersion(): SourceVersion = SourceVersion.latestSupported()

    override fun process(annotations: Set<TypeElement>, roundEnv: RoundEnvironment): Boolean {
        if (!roundEnv.processingOver()) {
            val connectors = discoverConnectors(roundEnv)
            processConfigurationProperties(roundEnv, connectors)
            ConfigurationMetadataStore.writeAdditionalMetadata(
                processingEnv,
                groups.values,
                properties.values
            )
            ConfigurationMetadataStore.writeConnectorsIndex(processingEnv, localConnectors)
        }
        return false
    }

    private fun discoverConnectors(roundEnv: RoundEnvironment): List<ConnectorDescriptor> {
        val candidateElements = mutableMapOf<String, TypeElement>()
        val elements = processingEnv.elementUtils
        val discoveredClassNames = scanClasspathForConnectors() +
            ConfigurationMetadataStore.readIndexedConnectors(processingEnv)
        for (className in discoveredClassNames) {
            elements.getTypeElement(className)?.let {
                candidateElements[it.qualifiedName.toString()] = it
            }
        }
        elements.getPackageElement(BOOTSTRAP_PACKAGE)?.enclosedElements?.forEach { enclosed ->
            if (enclosed is TypeElement) {
                candidateElements[enclosed.qualifiedName.toString()] = enclosed
            }
        }
        elements.getTypeElement(CONNECTOR_ANNOTATION)?.let { annotationType ->
            roundEnv.getElementsAnnotatedWith(annotationType).forEach { annotated ->
                if (annotated is TypeElement) {
                    val qualifiedName = annotated.qualifiedName.toString()
                    candidateElements[qualifiedName] = annotated
                    localConnectors.add(qualifiedName)
                }
            }
        }
        return candidateElements.values.flatMap { buildConnectorDescriptors(it) }
    }

    @Suppress("TooGenericExceptionCaught")
    private fun scanClasspathForConnectors(): Set<String> {
        val result = mutableSetOf<String>()
        try {
            val scanner = ClassPathScanningCandidateComponentProvider(false, StandardEnvironment())
            scanner.resourceLoader = PathMatchingResourcePatternResolver(
                ConnectorConfigurationMetadataProcessor::class.java.classLoader
            )
            scanner.addIncludeFilter(AnnotationTypeFilter(Connector::class.java))
            for (candidate in scanner.findCandidateComponents(BASE_SCAN_PACKAGE)) {
                candidate.beanClassName?.let { result.add(it) }
            }
        } catch (_: Exception) {
            // Fallback to package and index inspection if classpath scanning fails
        }
        return result
    }

    private fun buildConnectorDescriptors(element: TypeElement): List<ConnectorDescriptor> {
        val connectorPrefix = MetadataElementUtils.extractAnnotationStringValue(
            element,
            CONNECTOR_ANNOTATION,
            listOf("configurationPrefix")
        )
        val descriptors = mutableListOf<ConnectorDescriptor>()
        if (connectorPrefix != null) {
            val kebabPrefix = MetadataElementUtils.camelToKebab(connectorPrefix)
            val inflatorBinaryName = processingEnv.elementUtils.getBinaryName(element).toString()
            val sourceProps = MetadataElementUtils.resolveInflatorPropertiesType(
                processingEnv,
                element.asType(),
                SOURCE_INFLATOR_INTERFACE
            )
            if (sourceProps != null) {
                descriptors.add(ConnectorDescriptor(kebabPrefix, inflatorBinaryName, sourceProps, isSource = true))
            }
            val sinkProps = MetadataElementUtils.resolveInflatorPropertiesType(
                processingEnv,
                element.asType(),
                SINK_INFLATOR_INTERFACE
            )
            if (sinkProps != null) {
                descriptors.add(ConnectorDescriptor(kebabPrefix, inflatorBinaryName, sinkProps, isSource = false))
            }
        }
        return descriptors
    }

    private fun processConfigurationProperties(
        roundEnv: RoundEnvironment,
        connectors: List<ConnectorDescriptor>
    ) {
        val annotationType = processingEnv.elementUtils.getTypeElement(CONFIG_PROPS_ANNOTATION) ?: return
        for (element in roundEnv.getElementsAnnotatedWith(annotationType)) {
            if (element is TypeElement) {
                val rawPrefix = MetadataElementUtils.extractAnnotationStringValue(
                    element,
                    CONFIG_PROPS_ANNOTATION,
                    listOf("prefix", "value")
                ).orEmpty()
                val prefix = MetadataElementUtils.camelToKebab(rawPrefix)
                inspectConfigPropsElement(prefix, element, connectors, emptySet())
            }
        }
    }

    private fun inspectConfigPropsElement(
        prefix: String,
        ownerElement: TypeElement,
        connectors: List<ConnectorDescriptor>,
        visited: Set<String>
    ) {
        val ownerName = ownerElement.qualifiedName.toString()
        if (ownerName in visited) {
            return
        }
        val ownerBinaryName = processingEnv.elementUtils.getBinaryName(ownerElement).toString()
        val members = MetadataElementUtils.extractMemberProperties(ownerElement)
        for ((memberName, memberInfo) in members) {
            val kebabMember = MetadataElementUtils.camelToKebab(memberName)
            val propPrefix = if (prefix.isEmpty()) kebabMember else "$prefix.$kebabMember"
            val rawTypeName = ((memberInfo.first as? DeclaredType)?.asElement() as? TypeElement)
                ?.qualifiedName?.toString()
            if (rawTypeName == FLINK_SOURCE_INTERFACE) {
                addConnectorMetadata(propPrefix, memberInfo.first, ownerBinaryName, connectors.filter { it.isSource })
            } else if (rawTypeName == FLINK_SINK_INTERFACE) {
                addConnectorMetadata(propPrefix, memberInfo.first, ownerBinaryName, connectors.filter { !it.isSource })
            } else if (memberInfo.second) {
                val nestedElement = (memberInfo.first as? DeclaredType)?.asElement() as? TypeElement
                if (nestedElement != null) {
                    extractPropertiesFromType(propPrefix, nestedElement, visited + ownerName)
                }
            }
        }
    }

    private fun addConnectorMetadata(
        propPrefix: String,
        fieldType: TypeMirror,
        ownerBinaryName: String,
        matchingConnectors: List<ConnectorDescriptor>
    ) {
        properties[propPrefix] = MetadataProperty(
            name = propPrefix,
            type = MetadataElementUtils.formatPropertyType(processingEnv, fieldType),
            sourceType = ownerBinaryName
        )
        for (connector in matchingConnectors) {
            val connectorPrefix = "$propPrefix.${connector.connectorPrefix}"
            val propsBinaryName = processingEnv.elementUtils.getBinaryName(connector.propertiesType).toString()
            groups[connectorPrefix] = MetadataGroup(
                name = connectorPrefix,
                type = propsBinaryName,
                sourceType = connector.inflatorTypeName
            )
            extractPropertiesFromType(connectorPrefix, connector.propertiesType, emptySet())
        }
    }

    private fun extractPropertiesFromType(
        prefix: String,
        typeElement: TypeElement,
        visited: Set<String>
    ) {
        val qualifiedName = typeElement.qualifiedName.toString()
        if (qualifiedName in visited) {
            return
        }
        val ownerBinaryName = processingEnv.elementUtils.getBinaryName(typeElement).toString()
        val members = MetadataElementUtils.extractMemberProperties(typeElement)
        for ((memberName, memberInfo) in members) {
            val propName = "$prefix.${MetadataElementUtils.camelToKebab(memberName)}"
            val nestedClass = MetadataElementUtils.resolveNestedConfigClass(memberInfo.first)
            if (nestedClass != null) {
                val nestedBinaryName = processingEnv.elementUtils.getBinaryName(nestedClass).toString()
                groups[propName] = MetadataGroup(
                    name = propName,
                    type = nestedBinaryName,
                    sourceType = ownerBinaryName
                )
                extractPropertiesFromType(propName, nestedClass, visited + qualifiedName)
            } else {
                properties[propName] = MetadataProperty(
                    name = propName,
                    type = MetadataElementUtils.formatPropertyType(processingEnv, memberInfo.first),
                    sourceType = ownerBinaryName
                )
            }
        }
    }

    companion object {
        private const val BASE_SCAN_PACKAGE = "com.example"
        private const val BOOTSTRAP_PACKAGE = "com.example.flinkcommon.core.bootstrap"
        private const val CONNECTOR_ANNOTATION = "com.example.flinkcommon.core.bootstrap.Connector"
        private const val SOURCE_INFLATOR_INTERFACE = "com.example.flinkcommon.core.bootstrap.SourceInflator"
        private const val SINK_INFLATOR_INTERFACE = "com.example.flinkcommon.core.bootstrap.SinkInflator"
        private const val FLINK_SOURCE_INTERFACE = "org.apache.flink.api.connector.source.Source"
        private const val FLINK_SINK_INTERFACE = "org.apache.flink.api.connector.sink2.Sink"
        private const val CONFIG_PROPS_ANNOTATION =
            "org.springframework.boot.context.properties.ConfigurationProperties"
    }
}

internal object MetadataElementUtils {
    private const val GETTER_PREFIX_LEN = 3
    private const val NESTED_CONFIG_PROP_ANNOTATION =
        "org.springframework.boot.context.properties.NestedConfigurationProperty"
    private val IGNORED_METHODS = setOf("getClass", "getDeclaringClass")
    private val CAMEL_CASE_REGEX = Regex("([a-z0-9])([A-Z])")

    fun resolveInflatorPropertiesType(
        processingEnv: ProcessingEnvironment,
        typeMirror: TypeMirror,
        targetInterfaceName: String
    ): TypeElement? {
        val queue = ArrayDeque<TypeMirror>()
        val visited = mutableSetOf<String>()
        queue.add(typeMirror)
        var found: TypeElement? = null
        while (queue.isNotEmpty() && found == null) {
            val current = queue.removeFirst()
            val declared = current as? DeclaredType ?: continue
            val element = declared.asElement() as? TypeElement ?: continue
            val qualifiedName = element.qualifiedName.toString()
            if (qualifiedName == targetInterfaceName) {
                val typeArg = declared.typeArguments.firstOrNull() as? DeclaredType
                found = typeArg?.asElement() as? TypeElement
            } else if (visited.add(qualifiedName)) {
                queue.addAll(processingEnv.typeUtils.directSupertypes(current))
            }
        }
        return found
    }

    fun resolveNestedConfigClass(typeMirror: TypeMirror): TypeElement? {
        val declared = typeMirror as? DeclaredType
        val element = declared?.asElement() as? TypeElement
        val name = element?.qualifiedName?.toString().orEmpty()
        val isNestedCandidate = element != null &&
            element.kind == ElementKind.CLASS &&
            !name.startsWith("java.") &&
            !name.startsWith("kotlin.") &&
            !name.startsWith("org.apache.flink.") &&
            declared.typeArguments.isEmpty()
        return if (isNestedCandidate) element else null
    }

    fun extractMemberProperties(typeElement: TypeElement): Map<String, Pair<TypeMirror, Boolean>> {
        val result = mutableMapOf<String, Pair<TypeMirror, Boolean>>()
        for (enclosed in typeElement.enclosedElements) {
            if (enclosed.kind == ElementKind.FIELD && enclosed is VariableElement) {
                val name = enclosed.simpleName.toString()
                if (isValidField(enclosed, name)) {
                    val isNested = extractAnnotationStringValue(
                        enclosed,
                        NESTED_CONFIG_PROP_ANNOTATION,
                        emptyList()
                    ) != null || enclosed.annotationMirrors.any {
                        (it.annotationType.asElement() as? TypeElement)
                            ?.qualifiedName?.toString() == NESTED_CONFIG_PROP_ANNOTATION
                    }
                    result[name] = enclosed.asType() to isNested
                }
            }
        }
        for (enclosed in typeElement.enclosedElements) {
            if (enclosed.kind == ElementKind.METHOD && enclosed is ExecutableElement) {
                val propName = extractGetterPropertyName(enclosed)
                if (propName != null && propName !in result) {
                    val isNested = enclosed.annotationMirrors.any {
                        (it.annotationType.asElement() as? TypeElement)
                            ?.qualifiedName?.toString() == NESTED_CONFIG_PROP_ANNOTATION
                    }
                    result[propName] = enclosed.returnType to isNested
                }
            }
        }
        return result
    }

    private fun isValidField(field: VariableElement, name: String): Boolean {
        return Modifier.STATIC !in field.modifiers &&
            !name.contains('$') &&
            name != "Companion" &&
            name != "INSTANCE"
    }

    private fun extractGetterPropertyName(method: ExecutableElement): String? {
        val name = method.simpleName.toString()
        val isPublicGetter = Modifier.PUBLIC in method.modifiers &&
            Modifier.STATIC !in method.modifiers &&
            method.parameters.isEmpty() &&
            name !in IGNORED_METHODS &&
            name.startsWith("get") &&
            name.length > GETTER_PREFIX_LEN &&
            name[GETTER_PREFIX_LEN].isUpperCase()
        return if (isPublicGetter) {
            name.substring(GETTER_PREFIX_LEN).replaceFirstChar { it.lowercaseChar() }
        } else {
            null
        }
    }

    fun formatPropertyType(processingEnv: ProcessingEnvironment, typeMirror: TypeMirror): String {
        return when (typeMirror) {
            is PrimitiveType -> processingEnv.typeUtils.boxedClass(typeMirror).qualifiedName.toString()
            is DeclaredType -> formatDeclaredType(processingEnv, typeMirror)
            else -> typeMirror.toString()
        }
    }

    private fun formatDeclaredType(processingEnv: ProcessingEnvironment, typeMirror: DeclaredType): String {
        val element = typeMirror.asElement() as? TypeElement
        val baseName = if (element != null) {
            processingEnv.elementUtils.getBinaryName(element).toString()
        } else {
            typeMirror.toString()
        }
        val args = typeMirror.typeArguments
        return if (args.isEmpty()) {
            baseName
        } else {
            val formattedArgs = args.joinToString(",") { arg ->
                if (arg is WildcardType) "?" else formatPropertyType(processingEnv, arg)
            }
            "$baseName<$formattedArgs>"
        }
    }

    fun extractAnnotationStringValue(
        element: Element,
        annotationName: String,
        attributeNames: List<String>
    ): String? {
        val mirror = element.annotationMirrors.firstOrNull {
            (it.annotationType.asElement() as? TypeElement)?.qualifiedName?.toString() == annotationName
        }
        return mirror?.let { ann ->
            attributeNames.firstNotNullOfOrNull { attrName ->
                ann.elementValues.entries
                    .firstOrNull { it.key.simpleName.toString() == attrName }
                    ?.value
                    ?.value
                    ?.toString()
                    ?.takeIf { it.isNotEmpty() }
            }
        }
    }

    fun camelToKebab(input: String): String {
        return input.replace(CAMEL_CASE_REGEX, "$1-$2").lowercase()
    }
}

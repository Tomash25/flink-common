package com.example.flinkcommon.spring.processor

import javax.annotation.processing.ProcessingEnvironment
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

internal object MetadataElementUtils {
    private const val GETTER_PREFIX_LEN = 3
    private const val NESTED_CONFIG_PROP_ANNOTATION =
        "org.springframework.boot.context.properties.NestedConfigurationProperty"
    private val IGNORED_METHODS = setOf("getClass", "getDeclaringClass")
    private val CAMEL_CASE_REGEX = Regex("([a-z0-9])([A-Z])")

    fun extractSupertypeGenericArguments(
        processingEnv: ProcessingEnvironment,
        typeMirror: TypeMirror
    ): Map<String, List<TypeElement>> {
        val result = mutableMapOf<String, List<TypeElement>>()
        val queue = ArrayDeque<TypeMirror>().apply { this.add(typeMirror) }

        while (queue.isNotEmpty()) {
            val current = queue.removeFirst() as? DeclaredType
            val element = current?.asElement() as? TypeElement
            val qualifiedName = element?.qualifiedName?.toString()

            if (qualifiedName != null && qualifiedName !in result) {
                result[qualifiedName] = current.typeArguments.mapNotNull {
                    (it as? DeclaredType)?.asElement() as? TypeElement
                }
                queue.addAll(processingEnv.typeUtils.directSupertypes(current))
            }
        }

        return result
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
                if (this.isValidField(enclosed, name)) {
                    val isNested = this.extractAnnotationStringValue(
                        enclosed,
                        this.NESTED_CONFIG_PROP_ANNOTATION,
                        emptyList()
                    ) != null || enclosed.annotationMirrors.any {
                        (it.annotationType.asElement() as? TypeElement)
                            ?.qualifiedName?.toString() == this.NESTED_CONFIG_PROP_ANNOTATION
                    }
                    result[name] = enclosed.asType() to isNested
                }
            }
        }
        for (enclosed in typeElement.enclosedElements) {
            if (enclosed.kind == ElementKind.METHOD && enclosed is ExecutableElement) {
                val propName = this.extractGetterPropertyName(enclosed)
                if (propName != null && propName !in result) {
                    val isNested = enclosed.annotationMirrors.any {
                        (it.annotationType.asElement() as? TypeElement)
                            ?.qualifiedName?.toString() == this.NESTED_CONFIG_PROP_ANNOTATION
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
                name !in this.IGNORED_METHODS &&
                name.startsWith("get") &&
                name.length > this.GETTER_PREFIX_LEN &&
                name[this.GETTER_PREFIX_LEN].isUpperCase()
        return if (isPublicGetter) {
            name.substring(this.GETTER_PREFIX_LEN).replaceFirstChar { it.lowercaseChar() }
        } else {
            null
        }
    }

    fun formatPropertyType(processingEnv: ProcessingEnvironment, typeMirror: TypeMirror): String {
        return when (typeMirror) {
            is PrimitiveType -> processingEnv.typeUtils.boxedClass(typeMirror).qualifiedName.toString()
            is DeclaredType -> this.formatDeclaredType(processingEnv, typeMirror)
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
                if (arg is WildcardType) "?" else this.formatPropertyType(processingEnv, arg)
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
        return input.replace(this.CAMEL_CASE_REGEX, "$1-$2").lowercase()
    }
}

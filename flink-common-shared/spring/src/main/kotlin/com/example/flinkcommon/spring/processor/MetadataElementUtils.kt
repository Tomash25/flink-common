package com.example.flinkcommon.spring.processor

import javax.annotation.processing.ProcessingEnvironment
import javax.lang.model.element.ElementKind
import javax.lang.model.element.Modifier
import javax.lang.model.element.TypeElement
import javax.lang.model.element.VariableElement
import javax.lang.model.type.DeclaredType
import javax.lang.model.type.PrimitiveType
import javax.lang.model.type.TypeMirror
import javax.lang.model.type.WildcardType

internal object MetadataElementUtils {
    fun extractSupertypeGenericArguments(
        processingEnv: ProcessingEnvironment,
        typeMirror: TypeMirror
    ): Map<String, List<TypeElement>> {
        val supertypeArguments = mutableMapOf<String, List<TypeElement>>()
        val pendingSupertypes = ArrayDeque<TypeMirror>().apply { this.add(typeMirror) }

        while (pendingSupertypes.isNotEmpty()) {
            val current: DeclaredType? = pendingSupertypes.removeFirst() as? DeclaredType
            val element: TypeElement? = current?.asElement() as? TypeElement
            val qualifiedName: String? = element?.qualifiedName?.toString()

            if (qualifiedName != null && qualifiedName !in supertypeArguments) {
                supertypeArguments[qualifiedName] = current.typeArguments.mapNotNull {
                    (it as? DeclaredType)?.asElement() as? TypeElement
                }

                pendingSupertypes.addAll(processingEnv.typeUtils.directSupertypes(current))
            }
        }

        return supertypeArguments
    }

    fun resolveNestedConfigClass(typeMirror: TypeMirror): TypeElement? {
        val declared: DeclaredType? = typeMirror as? DeclaredType
        val element: TypeElement? = declared?.asElement() as? TypeElement
        val name: String = element?.qualifiedName?.toString().orEmpty()

        val isNestedCandidate = element != null &&
                element.kind == ElementKind.CLASS &&
                !name.startsWith("java.") &&
                !name.startsWith("kotlin.") &&
                !name.startsWith("org.apache.flink.") &&
                declared.typeArguments.isEmpty()

        return if (isNestedCandidate) element else null
    }

    fun extractMemberProperties(typeElement: TypeElement): Map<String, TypeMirror> {
        return typeElement.enclosedElements
            .filterIsInstance<VariableElement>()
            .filter {
                it.kind == ElementKind.FIELD &&
                        Modifier.STATIC !in it.modifiers &&
                        '$' !in it.simpleName
            }
            .associate { it.simpleName.toString() to it.asType() }
    }

    fun formatPropertyType(processingEnv: ProcessingEnvironment, typeMirror: TypeMirror): String {
        return when (typeMirror) {
            is PrimitiveType -> processingEnv.typeUtils.boxedClass(typeMirror).qualifiedName.toString()
            is WildcardType -> "?"
            is DeclaredType -> {
                val baseName: String = (typeMirror.asElement() as? TypeElement)
                    ?.let { processingEnv.elementUtils.getBinaryName(it).toString() }
                    ?: typeMirror.toString()

                val args: List<TypeMirror> = typeMirror.typeArguments

                if (args.isEmpty()) {
                    baseName
                } else {
                    val formattedArgs: String = args.joinToString(",") {
                        this.formatPropertyType(processingEnv, it)
                    }

                    "$baseName<$formattedArgs>"
                }
            }

            else -> typeMirror.toString()
        }
    }

    fun TypeMirror.qualifiedName(): String? =
        ((this as? DeclaredType)?.asElement() as? TypeElement)?.qualifiedName?.toString()
}

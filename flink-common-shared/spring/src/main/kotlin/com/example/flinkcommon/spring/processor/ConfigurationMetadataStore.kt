package com.example.flinkcommon.spring.processor

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import javax.annotation.processing.ProcessingEnvironment
import javax.tools.StandardLocation

internal data class MetadataGroup(
    val name: String,
    val type: String,
    val sourceType: String
)

internal data class MetadataProperty(
    val name: String,
    val type: String,
    val sourceType: String
)

internal object ConfigurationMetadataStore {
    private const val ADDITIONAL_METADATA_RESOURCE = "META-INF/additional-spring-configuration-metadata.json"
    private const val CONNECTORS_INDEX_RESOURCE = "META-INF/flink-common/connectors.idx"

    fun writeAdditionalMetadata(
        processingEnv: ProcessingEnvironment,
        groups: Collection<MetadataGroup>,
        properties: Collection<MetadataProperty>
    ) {
        if (groups.isEmpty() && properties.isEmpty()) {
            return
        }
        val json = serializeMetadata(groups, properties)
        val outputPaths = resolveOutputPaths(processingEnv)
        for (path in outputPaths) {
            writeToFile(path, json)
        }
    }

    fun writeConnectorsIndex(
        processingEnv: ProcessingEnvironment,
        connectorClassNames: Set<String>
    ) {
        if (connectorClassNames.isEmpty()) {
            return
        }
        val path = resolveResourcePath(processingEnv, StandardLocation.CLASS_OUTPUT, CONNECTORS_INDEX_RESOURCE)
        if (path != null) {
            val content = connectorClassNames.sorted().joinToString("\n") + "\n"
            writeToFile(path, content)
        }
    }

    @Suppress("TooGenericExceptionCaught")
    fun readIndexedConnectors(processingEnv: ProcessingEnvironment): Set<String> {
        val result = mutableSetOf<String>()
        try {
            val urls = ConfigurationMetadataStore::class.java.classLoader
                ?.getResources(CONNECTORS_INDEX_RESOURCE)
            while (urls != null && urls.hasMoreElements()) {
                urls.nextElement().openStream().bufferedReader(StandardCharsets.UTF_8).useLines { lines ->
                    lines.map { it.trim() }.filter { it.isNotEmpty() }.forEach { result.add(it) }
                }
            }
        } catch (_: Exception) {
            // Ignore classloader lookup errors
        }
        try {
            val fileObject = processingEnv.filer.getResource(
                StandardLocation.CLASS_PATH,
                "",
                CONNECTORS_INDEX_RESOURCE
            )
            fileObject.openInputStream().bufferedReader(StandardCharsets.UTF_8).useLines { lines ->
                lines.map { it.trim() }.filter { it.isNotEmpty() }.forEach { result.add(it) }
            }
        } catch (_: Exception) {
            // Ignore compiler classpath lookup errors when resource is absent
        }
        return result
    }

    private fun resolveOutputPaths(processingEnv: ProcessingEnvironment): Set<Path> {
        val paths = mutableSetOf<Path>()
        val classOutput = resolveResourcePath(
            processingEnv,
            StandardLocation.CLASS_OUTPUT,
            ADDITIONAL_METADATA_RESOURCE
        )
        val sourceOutput = resolveResourcePath(
            processingEnv,
            StandardLocation.SOURCE_OUTPUT,
            ADDITIONAL_METADATA_RESOURCE
        )
        if (classOutput != null) {
            paths.add(classOutput)
            addTargetGeneratedSourcePaths(classOutput, paths)
        }
        if (sourceOutput != null) {
            paths.add(sourceOutput)
        }
        return paths
    }

    private fun addTargetGeneratedSourcePaths(classOutputPath: Path, paths: MutableSet<Path>) {
        val targetDir = classOutputPath.parent?.parent?.parent ?: return
        if (targetDir.fileName?.toString() != "target") {
            return
        }
        val annotationsPath = targetDir
            .resolve("generated-sources")
            .resolve("annotations")
            .resolve(ADDITIONAL_METADATA_RESOURCE)
        paths.add(annotationsPath)
        if (Files.isDirectory(targetDir.resolve("kaptStubs"))) {
            val kaptPath = targetDir
                .resolve("generated-sources")
                .resolve("kapt")
                .resolve("compile")
                .resolve(ADDITIONAL_METADATA_RESOURCE)
            paths.add(kaptPath)
        }
    }

    @Suppress("TooGenericExceptionCaught")
    private fun resolveResourcePath(
        processingEnv: ProcessingEnvironment,
        location: StandardLocation,
        relativePath: String
    ): Path? {
        return try {
            val fileObject = processingEnv.filer.getResource(location, "", relativePath)
            Paths.get(fileObject.toUri())
        } catch (_: Exception) {
            null
        }
    }

    @Suppress("TooGenericExceptionCaught")
    private fun writeToFile(path: Path, content: String) {
        try {
            path.parent?.let { Files.createDirectories(it) }
            Files.write(path, content.toByteArray(StandardCharsets.UTF_8))
        } catch (_: Exception) {
            // Ignore write errors for optional output locations
        }
    }

    private fun serializeMetadata(
        groups: Collection<MetadataGroup>,
        properties: Collection<MetadataProperty>
    ): String {
        val sortedGroups = groups.distinctBy { it.name }.sortedBy { it.name }
        val sortedProperties = properties.distinctBy { it.name }.sortedBy { it.name }
        val sb = StringBuilder()
        sb.append("{\n")
        sb.append("  \"groups\": [\n")
        sortedGroups.forEachIndexed { index, group ->
            sb.append("    {\n")
            sb.append("      \"name\": \"").append(escapeJson(group.name)).append("\",\n")
            sb.append("      \"type\": \"").append(escapeJson(group.type)).append("\",\n")
            sb.append("      \"sourceType\": \"").append(escapeJson(group.sourceType)).append("\"\n")
            sb.append("    }")
            if (index < sortedGroups.size - 1) sb.append(",")
            sb.append("\n")
        }
        sb.append("  ],\n")
        sb.append("  \"properties\": [\n")
        sortedProperties.forEachIndexed { index, prop ->
            sb.append("    {\n")
            sb.append("      \"name\": \"").append(escapeJson(prop.name)).append("\",\n")
            sb.append("      \"type\": \"").append(escapeJson(prop.type)).append("\",\n")
            sb.append("      \"sourceType\": \"").append(escapeJson(prop.sourceType)).append("\"\n")
            sb.append("    }")
            if (index < sortedProperties.size - 1) sb.append(",")
            sb.append("\n")
        }
        sb.append("  ],\n")
        sb.append("  \"hints\": []\n")
        sb.append("}\n")
        return sb.toString()
    }

    private fun escapeJson(value: String): String {
        return value
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
    }
}

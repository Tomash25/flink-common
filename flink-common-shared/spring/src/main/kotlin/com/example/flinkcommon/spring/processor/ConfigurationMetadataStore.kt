package com.example.flinkcommon.spring.processor

import org.springframework.boot.configurationprocessor.metadata.ConfigurationMetadata
import org.springframework.boot.configurationprocessor.metadata.JsonMarshaller
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import javax.annotation.processing.ProcessingEnvironment
import javax.tools.StandardLocation

internal object ConfigurationMetadataStore {
    private const val ADDITIONAL_METADATA_RESOURCE = "META-INF/additional-spring-configuration-metadata.json"

    fun writeAdditionalMetadata(processingEnv: ProcessingEnvironment, metadata: ConfigurationMetadata) {
        if (metadata.items.isEmpty()) return

        val targetPath: Path = Paths.get(
            processingEnv.filer
                .getResource(StandardLocation.CLASS_OUTPUT, "", ADDITIONAL_METADATA_RESOURCE)
                .toUri()
        )

        targetPath.parent?.let {
            Files.createDirectories(it)
        }

        Files.newOutputStream(targetPath).use { outputStream ->
            JsonMarshaller().write(metadata, outputStream)
        }
    }
}

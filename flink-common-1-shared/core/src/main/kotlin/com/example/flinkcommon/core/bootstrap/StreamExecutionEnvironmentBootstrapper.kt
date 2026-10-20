package com.example.flinkcommon.core.bootstrap

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import com.example.flinkcommon.core.models.StreamExecutionEnvironmentProperties

class StreamExecutionEnvironmentBootstrapper {
    fun bootstrap(environment: StreamExecutionEnvironment, properties: StreamExecutionEnvironmentProperties) {
        properties.parallelism?.let { environment.parallelism = it }
        properties.maxParallelism?.let { environment.maxParallelism = it }
        environment.setRuntimeMode(properties.runtimeMode)
        environment.config.autoWatermarkInterval = properties.autoWatermarkInterval.toMillis()
        environment.configureCheckpointing(properties.checkpointing)
    }

    private fun StreamExecutionEnvironment.configureCheckpointing(
        properties: StreamExecutionEnvironmentProperties.CheckpointingProperties
    ) {
        this.checkpointing.apply {
            this.checkpointInterval = properties.interval.toMillis()
            this.checkpointingMode = properties.mode
            this.minPauseBetweenCheckpoints = properties.minPauseBetweenCp.toMillis()
            this.checkpointTimeout = properties.timeout.toMillis()
            this.maxConcurrentCheckpoints = properties.maxConcurrent
            this.tolerableCheckpointFailureNumber = properties.tolerableFailures
            this.enableUnalignedCheckpoints(properties.unaligned)
        }
    }
}

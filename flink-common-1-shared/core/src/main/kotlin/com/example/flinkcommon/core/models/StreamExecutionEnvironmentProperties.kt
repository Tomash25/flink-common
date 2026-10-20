package com.example.flinkcommon.core.models

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.CheckpointingMode
import java.time.Duration

data class StreamExecutionEnvironmentProperties(
    val parallelism: Int?,
    val maxParallelism: Int?,
    val runtimeMode: RuntimeExecutionMode = RuntimeExecutionMode.STREAMING,
    val autoWatermarkInterval: Duration,
    val checkpointing: CheckpointingProperties
) {
    data class CheckpointingProperties(
        val interval: Duration,
        val minPauseBetweenCp: Duration,
        val timeout: Duration,
        val maxConcurrent: Int,
        val tolerableFailures: Int,
        val unaligned: Boolean,
        val mode: CheckpointingMode
    )
}

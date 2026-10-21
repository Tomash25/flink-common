package com.example.flinkcommon.core.extensions

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.ConnectedStreams
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.KeyedStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag

fun <T, R> SingleOutputStreamOperator<T>.collectSideOutput(
    tag: OutputTag<R>
): Pair<SingleOutputStreamOperator<T>, DataStream<R>> {
    return this to this.getSideOutput<R>(tag)
}

inline fun <reified T> DataStream<Array<T>>.flatten(
    operatorName: String,
    operatorUid: String? = null
): SingleOutputStreamOperator<T> {
    return this
    .flatMap { items, collector -> items.forEach { collector.collect(it) } }
    .title(operatorName, operatorUid)
    .returns(T::class.java)
}

inline fun <reified T> DataStream<T?>.filterNonNull(
    operatorName: String,
    operatorUid: String,
): SingleOutputStreamOperator<T> {
    return this
        .filter { it != null }
        .title(operatorName, operatorUid)
        .map { it!! }
        .returns(T::class.java)
}

inline fun <reified T> DataStream<T?>.filterNonNull(
    operatorName: String
): SingleOutputStreamOperator<T> {
    return this
        .filter { it != null }
        .title(operatorName)
        .map { it!! }
        .returns(T::class.java)
}

inline fun <reified T> DataStream<T>.filterNonNull(
    operatorName: String,
    operatorUid: String,
    crossinline predicate: (T) -> Any?
): SingleOutputStreamOperator<T> {
    return this
        .filter { predicate(it) != null }
        .title(operatorName, operatorUid)
        .map { it!! }
        .returns(T::class.java)
}

inline fun <reified T> DataStream<T>.filterNonNull(
    operatorName: String,
    crossinline predicate: (T) -> Any?
): SingleOutputStreamOperator<T> {
    return this
        .filter { predicate(it) != null }
        .title(operatorName)
        .map { it!! }
        .returns(T::class.java)
}

inline fun <T, reified R> DataStream<T>.map(
    operatorName: String,
    operatorUid: String,
    mapper: MapFunction<T, R>
): SingleOutputStreamOperator<R> {
    return this
        .map(mapper, TypeInformation.of(R::class.java))
        .title(operatorName, operatorUid)
}

inline fun <T, reified R> DataStream<T>.map(
    operatorName: String,
    mapper: MapFunction<T, R>
): SingleOutputStreamOperator<R> {
    return this
        .map(mapper, TypeInformation.of(R::class.java))
        .title(operatorName)
}

inline fun <K, IN1, IN2, reified R> ConnectedStreams<IN1, IN2>.process(
    operatorName: String,
    operatorUid: String,
    keyedCoProcessFunction: KeyedCoProcessFunction<K, IN1, IN2, R>
): SingleOutputStreamOperator<R> {
    return this
        .process(keyedCoProcessFunction, TypeInformation.of(R::class.java))
        .title(operatorName, operatorUid)
}

inline fun <K, IN1, IN2, reified R> ConnectedStreams<IN1, IN2>.process(
    operatorName: String,
    keyedCoProcessFunction: KeyedCoProcessFunction<K, IN1, IN2, R>
): SingleOutputStreamOperator<R> {
    return this
        .process(keyedCoProcessFunction, TypeInformation.of(R::class.java))
        .title(operatorName)
}

fun <T> DataStream<T>.filter(
    operatorName: String,
    operatorUid: String,
    filterFunction: FilterFunction<T>
): SingleOutputStreamOperator<T> {
    return this
        .filter(filterFunction)
        .title(operatorName, operatorUid)
}

fun <T> DataStream<T>.filter(
    operatorName: String,
    filterFunction: FilterFunction<T>
): SingleOutputStreamOperator<T> {
    return this
        .filter(filterFunction)
        .title(operatorName)
}

inline fun <T, reified R> DataStream<T>.process(
    operatorName: String,
    operatorUid: String,
    processFunction: ProcessFunction<T, R>
): SingleOutputStreamOperator<R> {
    return this
        .process(processFunction, TypeInformation.of(R::class.java))
        .title(operatorName, operatorUid)
}

inline fun <T, reified R> DataStream<T>.process(
    operatorName: String,
    processFunction: ProcessFunction<T, R>
): SingleOutputStreamOperator<R> {
    return this
        .process(processFunction, TypeInformation.of(R::class.java))
        .title(operatorName)
}

inline fun <reified T> DataStream<T>.tag(
    operatorName: String,
    operatorUid: String,
    crossinline tagSelector: (T) -> OutputTag<T>
): SingleOutputStreamOperator<T> {
    return this
        .process(
            object : ProcessFunction<T, T>() {
                override fun processElement(value: T, context: ProcessFunction<T, T>.Context, out: Collector<T>) =
                    context.output(tagSelector.invoke(value), value)
            },
            TypeInformation.of(T::class.java)
        )
        .title(operatorName, operatorUid)
}

inline fun <reified T> DataStream<T>.tag(
    operatorName: String,
    crossinline tagSelector: (T) -> OutputTag<T>
): SingleOutputStreamOperator<T> {
    return this
        .process(
            object : ProcessFunction<T, T>() {
                override fun processElement(value: T, context: ProcessFunction<T, T>.Context, out: Collector<T>) =
                    context.output(tagSelector.invoke(value), value)
            },
            TypeInformation.of(T::class.java)
        )
        .title(operatorName)
}

inline fun <T, KEY, reified R> KeyedStream<T, KEY>.process(
    operatorName: String,
    operatorUid: String,
    keyedProcessFunction: KeyedProcessFunction<KEY, T, R>
): SingleOutputStreamOperator<R> {
    return this
        .process(keyedProcessFunction, TypeInformation.of(R::class.java))
        .title(operatorName, operatorUid)
}

inline fun <T, KEY, reified R> KeyedStream<T, KEY>.process(
    operatorName: String,
    keyedProcessFunction: KeyedProcessFunction<KEY, T, R>
): SingleOutputStreamOperator<R> {
    return this
        .process(keyedProcessFunction, TypeInformation.of(R::class.java))
        .title(operatorName)
}

fun <T> SingleOutputStreamOperator<T>.title(name: String, uid: String? = null): SingleOutputStreamOperator<T> {
    return this
        .name(name)
        .uid(uid ?: normalizeUid(name))
}

fun normalizeUid(operatorUid: String): String = operatorUid.replace(' ', '_')

package com.aygavras.beam

import mu.KotlinLogging
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Create
import org.joda.time.DateTime

private val logger = KotlinLogging.logger {}

fun main(args: Array<String>) {

    val options = PipelineOptionsFactory.fromArgs(*args).create()
    val pipeline = Pipeline.create(options)

    while(true) {
        val webEvents = Create.of(
                WebEvent(DateTime.now(), "IE8"),
                WebEvent(DateTime.now(), "Firefox"),
                WebEvent(DateTime.now(), "Chrome"),
                WebEvent(DateTime.now(), "IE8"),
                WebEvent(DateTime.now(), "Firefox"),
                WebEvent(DateTime.now(), "Chrome")
        )

        // pipeline.begin().equals("ABC")

        val bootstrapServers = "localhost:9093,localhost:9094,localhost:9095"
        val kafkaWriter = KafkaIO.write<String, WebEvent>().withBootstrapServers(bootstrapServers)
                .withTopic("interop-topic")
                // .withKeySerializer(StringSerializer::class.java)
                .withValueSerializer(WebEventSerializer::class.java)
                .values()


        pipeline.apply(webEvents)
                /*.apply(ParDo.of(object : DoFn<WebEvent, KV<String, WebEvent>>() {
                @ProcessElement
                fun processElement(context: ProcessContext) {
                    val webEvent = context.element()
                    context.output(KV.of(webEvent.browser, webEvent))
                }
            }))*/
                /* .apply(Count.perKey())
             .apply(ParDo.of(object : DoFn<KV<String, Long>, String>() {
                 @ProcessElement
                 fun processElement(context: ProcessContext) {
                     logger.info(context.element().toString())
                 }
             }))*/
                .apply(kafkaWriter)
          pipeline.run()

    }
}

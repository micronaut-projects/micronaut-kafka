package io.micronaut.kafka.docs.streams

// tag::imports[]
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import java.util.*
// end::imports[]

@Requires(property = "spec.name", value = "WordCountStreamTest")
// tag::clazz[]
@Factory
class WordCountStream {
    companion object {
        const val STREAM_WORD_COUNT = "word-count"
        const val INPUT = "streams-plaintext-input" // <1>
        const val OUTPUT = "streams-wordcount-output" // <2>
        const val WORD_COUNT_STORE = "word-count-store"

        const val MY_STREAM = "my-stream"
        const val NAMED_WORD_COUNT_INPUT = "named-word-count-input"
        const val NAMED_WORD_COUNT_OUTPUT = "named-word-count-output"
    }
    // end::clazz[]

    // tag::wordCountStream[]
    @Singleton
    @Named(STREAM_WORD_COUNT)
    fun wordCountStream(builder: ConfiguredStreamBuilder): KStream<String, String> { // <3>
        // set default serdes
        val props = builder.configuration
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.getName()
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.getName()
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        props[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = "500"
        val source = builder.stream<String, String>(INPUT)
        val groupedByWord = source
            .flatMapValues { value: String ->
                Arrays.asList(
                    *value.lowercase(Locale.getDefault()).split("\\W+".toRegex()).dropLastWhile { it.isEmpty() }
                        .toTypedArray())
            }
            .groupBy(
                { key: String?, word: String? -> word },
                Grouped.with(Serdes.String(), Serdes.String())
            )
            //Store the result in a store for lookup later
            .count(Materialized.`as`(WORD_COUNT_STORE)) // <4>
        groupedByWord
            //convert to stream
            .toStream()
            //send to output using specific serdes
            .to(OUTPUT, Produced.with(Serdes.String(), Serdes.Long()))
        return source
    }
    // end::wordCountStream[]

    // tag::namedStream[]
    @Singleton
    @Named(MY_STREAM)
    fun myStream(
        @Named(MY_STREAM) builder: ConfiguredStreamBuilder
    ): KStream<String, String> {

        // end::namedStream[]
        // set default serdes
        val props = builder.configuration
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.getName()
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.getName()
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        props[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = "500"
        val source = builder.stream<String, String>(NAMED_WORD_COUNT_INPUT)
        val counts = source
            .flatMapValues { value: String ->
                Arrays.asList(
                    *value.lowercase(Locale.getDefault()).split(" ".toRegex()).dropLastWhile { it.isEmpty() }
                        .toTypedArray())
            }
            .groupBy { key: String?, value: String? -> value }
            .count()

        // need to override value serde to Long type
        counts.toStream().to(NAMED_WORD_COUNT_OUTPUT, Produced.with(Serdes.String(), Serdes.Long()))
        return source
    }
}

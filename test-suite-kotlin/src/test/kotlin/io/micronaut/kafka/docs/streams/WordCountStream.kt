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
    // end::clazz[]

    // tag::wordCountStream[]
    @Singleton
    @Named("word-count")
    fun wordCountStream(builder: ConfiguredStreamBuilder): KStream<String, String> { // <1>
        // set default serdes
        val props = builder.configuration
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.getName()
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.getName()
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        props[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = "500"
        val source = builder.stream<String, String>("streams-plaintext-input") // <2>
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
            .count(Materialized.`as`("word-count-store-kotlin")) // <3>
        groupedByWord
            //convert to stream
            .toStream()
            //send to output using specific serdes
            .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long())) // <4>
        return source
    }
    // end::wordCountStream[]

    // tag::namedStream[]
    @Singleton
    @Named("my-stream")
    fun myStream(builder: ConfiguredStreamBuilder): KStream<String, String> {
        // end::namedStream[]

        // set default serdes
        val props = builder.configuration
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.getName()
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.getName()
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        props[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = "500"
        val source = builder.stream<String, String>("named-word-count-input")
        val counts = source
            .flatMapValues { value: String ->
                Arrays.asList(
                    *value.lowercase(Locale.getDefault()).split(" ".toRegex()).dropLastWhile { it.isEmpty() }
                        .toTypedArray())
            }
            .groupBy { key: String?, value: String? -> value }
            .count()

        // need to override value serde to Long type
        counts.toStream().to("named-word-count-output", Produced.with(Serdes.String(), Serdes.Long()))
        return source
    }

    // tag::myOtherStream[]
    @Singleton
    @Named("my-other-stream")
    fun myOtherKStream(builder: ConfiguredStreamBuilder): KStream<String, String> {
        return builder.stream("my-other-stream")
    }
    // end::myOtherStream[]
}

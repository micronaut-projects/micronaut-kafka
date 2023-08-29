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
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
// end::imports[]

@Requires(property = 'spec.name', value = 'WordCountStreamTest')
// tag::clazz[]
@Factory
class WordCountStream {
// end::clazz[]

    // tag::wordCountStream[]
    @Singleton
    @Named('word-count')
    KStream<String, String> wordCountStream(ConfiguredStreamBuilder builder) { // <1>
        // set default serdes
        Properties props = builder.getConfiguration();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 'earliest')
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, '500')

        KStream<String, String> source = builder.stream('streams-plaintext-input') // <2>

        KTable<String, Long> groupedByWord = source
            .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
            .groupBy((key, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
            //Store the result in a store for lookup later
            .count(Materialized.as('word-count-store-groovy')) // <3>

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
    @Named('my-stream')
    KStream<String, String> myStream(ConfiguredStreamBuilder builder) {

        // end::namedStream[]
        // set default serdes
        Properties props = builder.getConfiguration()
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 'earliest')
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, '500')

        KStream<String, String> source = builder.stream("named-word-count-input")
        KTable<String, Long> counts = source
            .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
            .groupBy((key, value) -> value)
            .count()

        // need to override value serde to Long type
        counts.toStream().to("named-word-count-output", Produced.with(Serdes.String(), Serdes.Long()));
        return source
    }

    // tag::myOtherStream[]
    @Singleton
    @Named('my-other-stream')
    KStream<String, String> myOtherKStream(ConfiguredStreamBuilder builder)  {
        return builder.stream('my-other-stream')
    }
    // end::myOtherStream[]
}

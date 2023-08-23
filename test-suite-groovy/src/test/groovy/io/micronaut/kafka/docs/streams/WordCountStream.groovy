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

    public static final String STREAM_WORD_COUNT = 'word-count'
    public static final String INPUT = 'streams-plaintext-input' // <1>
    public static final String OUTPUT = 'streams-wordcount-output' // <2>
    public static final String WORD_COUNT_STORE = 'word-count-store'
// end::clazz[]

    // tag::wordCountStream[]
    @Singleton
    @Named(STREAM_WORD_COUNT)
    KStream<String, String> wordCountStream(ConfiguredStreamBuilder builder) { // <3>
        // set default serdes
        Properties props = builder.getConfiguration();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 'earliest')
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, '500')

        KStream<String, String> source = builder.stream(INPUT)

        KTable<String, Long> groupedByWord = source
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
        //Store the result in a store for lookup later
                .count(Materialized.as(WORD_COUNT_STORE)) // <4>

        groupedByWord
        //convert to stream
                .toStream()
        //send to output using specific serdes
                .to(OUTPUT, Produced.with(Serdes.String(), Serdes.Long()))

        return source
    }
    // end::wordCountStream[]

    // tag::namedStream[]
    public static final String MY_STREAM = 'my-stream'
    public static final String NAMED_WORD_COUNT_INPUT = 'named-word-count-input'
    public static final String NAMED_WORD_COUNT_OUTPUT = 'named-word-count-output'

    @Singleton
    @Named(MY_STREAM)
    KStream<String, String> myStream(
            @Named(MY_STREAM) ConfiguredStreamBuilder builder) {

        // end::namedStream[]
        // set default serdes
        Properties props = builder.getConfiguration()
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 'earliest')
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, '500')

        KStream<String, String> source = builder.stream(NAMED_WORD_COUNT_INPUT)
        KTable<String, Long> counts = source
                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
                .groupBy((key, value) -> value)
                .count()

        // need to override value serde to Long type
        counts.toStream().to(NAMED_WORD_COUNT_OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));
        return source
    }
}

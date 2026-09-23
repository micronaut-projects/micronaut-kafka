# tag::imports[]
import re
from typing import Annotated

from jakarta.inject import Named, Singleton
from micronaut.configuration.kafka.streams import ConfiguredStreamBuilder
from micronaut.context.annotation import Factory, Requires
from org.apache.kafka.clients.consumer import ConsumerConfig
from org.apache.kafka.common.serialization import Serdes
from org.apache.kafka.streams import StreamsConfig
from org.apache.kafka.streams.kstream import Grouped, KStream, Materialized, Produced
# end::imports[]


@Requires(property="spec.name", value="WordCountStreamTest")
# tag::clazz[]
@Factory
class WordCountStream:
# end::clazz[]

    # tag::wordCountStream[]
    @Singleton
    @Named("word-count")
    def word_count_stream(self, builder: ConfiguredStreamBuilder) -> KStream[str, str]:  # <1>
        # set default serdes
        props = builder.getConfiguration()
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500")

        source = builder.stream("streams-plaintext-input")  # <2>

        grouped_by_word = (source
            .flatMapValues(lambda value: re.split(r"\W+", value.lower()))
            .groupBy(lambda key, word: word, Grouped.with_(Serdes.String(), Serdes.String()))
            # Store the result in a store for lookup later
            .count(Materialized.as_("word-count-store-python")))  # <3>

        (grouped_by_word
            # convert to stream
            .toStream()
            # send to output using specific serdes
            .to("streams-wordcount-output", Produced.with_(Serdes.String(), Serdes.Long())))  # <4>

        return source
    # end::wordCountStream[]

    # tag::namedStream[]
    @Singleton
    @Named("my-stream")
    def my_stream(self, builder: Annotated[ConfiguredStreamBuilder, Named("my-stream")]) -> KStream[str, str]:

        # end::namedStream[]
        # set default serdes
        props = builder.getConfiguration()
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500")

        source = builder.stream("named-word-count-input")
        counts = (source
            .flatMapValues(lambda value: value.lower().split(" "))
            .groupBy(lambda key, value: value)
            .count())

        # need to override value serde to Long type
        counts.toStream().to("named-word-count-output", Produced.with_(Serdes.String(), Serdes.Long()))
        return source

    # tag::myOtherStream[]
    @Singleton
    @Named("my-other-stream")
    def my_other_kstream(self, builder: Annotated[ConfiguredStreamBuilder, Named("my-other-stream")]) -> KStream[str, str]:
        return builder.stream("my-other-stream")
    # end::myOtherStream[]

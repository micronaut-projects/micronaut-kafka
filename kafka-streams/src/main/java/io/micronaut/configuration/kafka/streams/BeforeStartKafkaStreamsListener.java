package io.micronaut.configuration.kafka.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;

public interface BeforeStartKafkaStreamsListener {

    void execute(KafkaStreams kafkaStreams, KStream[] kStreams);
}

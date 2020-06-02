package io.micronaut.configuration.kafka.streams.optimization;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;

@KafkaClient
public interface OptimizationClient {

    @Topic(OptimizationStream.OPTIMIZATION_ON_INPUT)
    void publishOptimizationOnMessage(@KafkaKey String key, String value);

    @Topic(OptimizationStream.OPTIMIZATION_OFF_INPUT)
    void publishOptimizationOffMessage(@KafkaKey String key, String value);
}

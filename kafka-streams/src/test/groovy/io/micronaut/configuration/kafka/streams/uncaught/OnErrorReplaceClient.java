package io.micronaut.configuration.kafka.streams.uncaught;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;

@Requires(property = "spec.name", value = "UncaughtExceptionsSpec")
@KafkaClient
public interface OnErrorReplaceClient {

    @Topic(OnErrorStreamFactory.ON_ERROR_REPLACE_INPUT)
    void send(String message);
}

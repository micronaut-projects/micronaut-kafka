package io.micronaut.configuration.kafka.streams.uncaught;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST;

@Requires(property = "spec.name", value = "UncaughtExceptionsSpec")
@KafkaListener(offsetReset = EARLIEST, groupId = "OnErrorNoConfigListener", uniqueGroupId = true)
public class OnErrorNoConfigListener {

    public String received;

    @Topic(OnErrorStreamFactory.ON_ERROR_NO_CONFIG_OUTPUT)
    void receive(String message) {
        received = message;
    }
}

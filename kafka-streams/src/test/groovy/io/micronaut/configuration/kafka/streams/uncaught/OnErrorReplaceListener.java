package io.micronaut.configuration.kafka.streams.uncaught;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST;

@Requires(property = "spec.name", value = "UncaughtExceptionsSpec")
@KafkaListener(offsetReset = EARLIEST, groupId = "OnErrorReplaceListener", uniqueGroupId = true)
public class OnErrorReplaceListener {

    public String received;

    @Topic(OnErrorStreamFactory.ON_ERROR_REPLACE_OUTPUT)
    void receive(String message) {
        received = message;
    }
}

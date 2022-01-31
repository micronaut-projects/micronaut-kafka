package io.micronaut.test;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;

import static io.micronaut.core.util.StringUtils.TRUE;

@Requires(property = "kafka.enabled", notEquals = TRUE, defaultValue = TRUE)
@Replaces(DisabledClient.class)
@KafkaClient
public class DisabledClientFallback implements DisabledClient {
    @Override
    public void send(String message) {
        System.out.println("No-Op");
    }
}

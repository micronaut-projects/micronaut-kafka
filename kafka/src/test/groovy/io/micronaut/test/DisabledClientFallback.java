package io.micronaut.test;

import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.util.StringUtils;

@Requires(property = "kafka.enabled", notEquals = StringUtils.TRUE, defaultValue = StringUtils.TRUE)
@Replaces(DisabledClient.class)
public class DisabledClientFallback implements DisabledClient {
    @Override
    public void send(String message) {
        System.out.println("No-Op");
    }
}

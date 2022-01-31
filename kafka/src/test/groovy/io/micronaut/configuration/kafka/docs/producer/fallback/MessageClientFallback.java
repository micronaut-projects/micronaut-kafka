package io.micronaut.configuration.kafka.docs.producer.fallback;

// tag::imports[]
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.util.StringUtils;
// end::imports[]

// tag::clazz[]
@Requires(property = "kafka.enabled", notEquals = StringUtils.TRUE, defaultValue = StringUtils.TRUE) // <1>
@Replaces(MessageClient.class) // <2>
public class MessageClientFallback implements MessageClient { // <3>

    @Override
    public void send(String message) {
        throw new UnsupportedOperationException(); // <4>
    }
}
// end::clazz[]


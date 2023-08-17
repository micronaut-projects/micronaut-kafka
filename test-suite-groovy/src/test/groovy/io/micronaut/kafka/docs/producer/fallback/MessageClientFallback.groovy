package io.micronaut.kafka.docs.producer.fallback

// tag::imports[]
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.core.util.StringUtils
import jakarta.inject.Singleton
// end::imports[]

@Requires(property = "spec.name", value = "MessageClientFallbackSpec")
// tag::clazz[]
@Requires(property = 'kafka.enabled', notEquals = StringUtils.TRUE, defaultValue = StringUtils.TRUE) // <1>
@Replaces(MessageClient.class) // <2>
@Singleton
class MessageClientFallback implements MessageClient { // <3>

    @Override
    void send(String message) {
        throw new UnsupportedOperationException() // <4>
    }
}
// end::clazz[]

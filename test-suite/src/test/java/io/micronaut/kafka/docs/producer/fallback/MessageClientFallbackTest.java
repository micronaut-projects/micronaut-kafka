package io.micronaut.kafka.docs.producer.fallback;

import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Property;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
@Property(name = "spec.name", value = "MessageClientFallbackTest")
@Property(name = "kafka.enabled", value = "false")
class MessageClientFallbackTest {

    @Inject
    BeanContext context;

    @Test
    void contextContainsFallbackBean() {
        MessageClientFallback bean = context.getBean(MessageClientFallback.class);

        assertNotNull(bean);
        assertThrows(UnsupportedOperationException.class, () -> bean.send("message"));
    }
}

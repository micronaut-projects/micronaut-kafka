package io.micronaut.kafka.docs.quickstart;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Property;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import io.micronaut.testcontainers.kafka.Kafka;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

@Property(name = "spec.name", value = "QuickstartTest")
@Property(name = "kafka.enabled", value = "true")
@MicronautTest
class QuickstartTest implements TestPropertyProvider {
    @Inject
    ApplicationContext applicationContext;

    @Override
    public Map<String, String> getProperties() {
        return Kafka.getProperties();
    }

    @Test
    void testSendProduct() {
        // tag::quickstart[]
        ProductClient client = applicationContext.getBean(ProductClient.class);
        client.sendProduct("Nike", "Blue Trainers");
        // end::quickstart[]
    }
}

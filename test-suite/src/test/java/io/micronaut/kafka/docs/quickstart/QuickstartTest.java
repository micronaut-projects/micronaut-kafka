package io.micronaut.kafka.docs.quickstart;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Property;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@Property(name = "spec.name", value = "QuickstartTest")
@Property(name = "kafka.enabled", value = "true")
@MicronautTest
class QuickstartTest {
    @Inject
    ApplicationContext applicationContext;

    @Test
    void testSendProduct() {
        // tag::quickstart[]
        ProductClient client = applicationContext.getBean(ProductClient.class);
        client.sendProduct("Nike", "Blue Trainers");
        // end::quickstart[]
    }
}

package io.micronaut.kafka.docs.consumer.scope;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.KafkaScope;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import jakarta.inject.Inject;
// end::imports[]

// tag::listener[]
@KafkaListener(offsetReset = OffsetReset.EARLIEST)
public class ProductListener {

    // tag::scope[]
    @KafkaScope
    static class ProductMetadata {
        private final String correlationId = java.util.UUID.randomUUID().toString();

        String getCorrelationId() {
            return correlationId;
        }
    }
    // end::scope[]

    @Inject ProductMetadata productMetadata;

    @Topic("products")
    void receive(String product) {
        String correlationId = productMetadata.getCorrelationId();
        // use the same metadata instance throughout this listener invocation
    }
}
// end::listener[]

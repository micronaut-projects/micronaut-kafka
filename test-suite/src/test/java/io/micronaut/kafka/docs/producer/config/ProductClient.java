package io.micronaut.kafka.docs.producer.config;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import org.apache.kafka.clients.producer.ProducerConfig;
// end::imports[]

@Requires(property = "spec.name", value = "ProductClientTest")
// tag::clazz[]
@KafkaClient(
    id = "product-client",
    acks = KafkaClient.Acknowledge.ALL,
    properties = @Property(name = ProducerConfig.RETRIES_CONFIG, value = "5")
)
public interface ProductClient {
    // define client API
}
// end::clazz[]

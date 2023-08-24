package io.micronaut.kafka.docs.producer.config;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.context.annotation.Requires;

@Requires(property = "spec.name", value = "ClientIdClientTest")
// tag::annotation[]
@KafkaClient("product-client")
// end::annotation[]
public interface ClientIdClient {
    // define client API
}

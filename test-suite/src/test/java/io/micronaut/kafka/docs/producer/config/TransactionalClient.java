package io.micronaut.kafka.docs.producer.config;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.context.annotation.Requires;

@Requires(property = "spec.name", value = "TransactionalClientTest")
// tag::clazz[]
@KafkaClient(id = "my-client", transactionalId = "my-tx-id")
public interface TransactionalClient {
    // define client API
}
// end::clazz[]

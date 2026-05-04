package io.micronaut.kafka.docs.consumer.errors;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;

@Requires(property = "spec.name", value = "RetryTopicProductListenerTest")
// tag::client[]
@KafkaClient("product-client")
public interface RetryTopicProductClient {

    @Topic("products")
    void sendProduct(String product);
}
// end::client[]

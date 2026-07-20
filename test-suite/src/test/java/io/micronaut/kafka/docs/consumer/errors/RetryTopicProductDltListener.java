package io.micronaut.kafka.docs.consumer.errors;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;
import io.micronaut.messaging.MessageHeaders;

@Requires(property = "spec.name", value = "RetryTopicProductListenerTest")
// tag::dlt[]
@KafkaListener("product-retry-dlt-group")
public class RetryTopicProductDltListener {

    @Topic("products-dlt")
    void receive(String product, MessageHeaders headers) {
        String originalTopic = headers.get("micronaut-kafka-original-topic", String.class).orElse("unknown");
        System.out.printf("Routing %s from %s to the dead letter topic%n", product, originalTopic);
    }
}
// end::dlt[]

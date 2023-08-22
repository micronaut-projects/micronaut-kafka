package io.micronaut.kafka.docs.consumer.config;

import io.micronaut.configuration.kafka.annotation.KafkaListener;

// tag::annotation[]
@KafkaListener("myGroup")
// end::annotation[]
public class ConsumerGroupListener {
    // define topic listeners
}

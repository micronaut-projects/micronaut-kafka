package io.micronaut.kafka.docs.consumer.topics;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record Product(String name) {}

package io.micronaut.kafka.docs.consumer.reactive;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record Product(String name) {}

package io.micronaut.kafka.docs.consumer.config;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record Product(String name, int quantity) {}

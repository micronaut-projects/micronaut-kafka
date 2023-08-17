package io.micronaut.kafka.docs;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record Product (String name, int quantity) { }

package io.micronaut.kafka.docs.producer.inject;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record Book (String title) {}

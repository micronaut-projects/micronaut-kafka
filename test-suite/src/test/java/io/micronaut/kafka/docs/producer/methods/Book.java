package io.micronaut.kafka.docs.producer.methods;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record Book(String title) {
}

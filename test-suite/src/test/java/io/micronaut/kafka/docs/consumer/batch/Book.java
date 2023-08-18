package io.micronaut.kafka.docs.consumer.batch;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record Book (String title) {
}

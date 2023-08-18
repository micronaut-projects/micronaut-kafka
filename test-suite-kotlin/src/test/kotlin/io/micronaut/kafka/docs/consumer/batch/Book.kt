package io.micronaut.kafka.docs.consumer.batch

import io.micronaut.serde.annotation.Serdeable

@Serdeable
data class Book(val title: String)

package io.micronaut.kafka.docs.producer.inject

import io.micronaut.serde.annotation.Serdeable

@Serdeable
data class Book(val title: String)

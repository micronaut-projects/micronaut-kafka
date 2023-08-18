package io.micronaut.kafka.docs.consumer.sendto

import io.micronaut.serde.annotation.Serdeable

@Serdeable
data class Product(val name: String, val quantity: Int)

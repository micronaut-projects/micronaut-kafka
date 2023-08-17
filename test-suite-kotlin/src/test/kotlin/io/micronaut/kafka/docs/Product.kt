package io.micronaut.kafka.docs

import io.micronaut.serde.annotation.Serdeable

@Serdeable
data class Product(val name: String? = null, val quantity: Int = 0) { }

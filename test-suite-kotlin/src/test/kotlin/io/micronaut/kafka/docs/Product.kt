package io.micronaut.kafka.docs

import io.micronaut.serde.annotation.Serdeable

@Serdeable
class Product {
    var name: String? = null
    var quantity = 0
}

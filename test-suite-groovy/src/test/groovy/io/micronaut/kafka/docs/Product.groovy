package io.micronaut.kafka.docs

import io.micronaut.serde.annotation.Serdeable

@Serdeable
class Product {

    String name
    int quantity
}

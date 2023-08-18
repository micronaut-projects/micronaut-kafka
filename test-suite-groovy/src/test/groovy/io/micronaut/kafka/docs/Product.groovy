package io.micronaut.kafka.docs

import groovy.transform.Canonical
import io.micronaut.serde.annotation.Serdeable

@Serdeable
@Canonical
class Product {
    String name
    int quantity
}

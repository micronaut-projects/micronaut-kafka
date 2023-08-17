package io.micronaut.kafka.docs

import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import io.micronaut.serde.annotation.Serdeable

@Serdeable
@ToString
@EqualsAndHashCode
class Product {

    String name
    int quantity

    Product(String name, int quantity) {
        this.name = name
        this.quantity = quantity
    }
}

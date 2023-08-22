package io.micronaut.kafka.docs.consumer.topics

import groovy.transform.Canonical
import io.micronaut.serde.annotation.Serdeable

@Serdeable
@Canonical
class Product {
    String name
}

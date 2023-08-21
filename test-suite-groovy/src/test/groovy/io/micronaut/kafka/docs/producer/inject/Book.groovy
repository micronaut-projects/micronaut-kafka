package io.micronaut.kafka.docs.producer.inject

import groovy.transform.Canonical
import io.micronaut.serde.annotation.Serdeable

@Serdeable
@Canonical
class Book {
    String title
}

package io.micronaut.kafka.docs.producer.inject

import io.micronaut.serde.annotation.Serdeable

@Serdeable
class Book {
    String title

    Book(String title) {
        this.title = title
    }
}

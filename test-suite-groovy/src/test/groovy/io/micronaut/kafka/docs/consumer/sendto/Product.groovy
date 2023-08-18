package io.micronaut.kafka.docs.consumer.sendto

import io.micronaut.serde.annotation.Serdeable

@Serdeable
class Product {

    private String name;
    private int quantity;
}

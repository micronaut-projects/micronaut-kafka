package io.micronaut.configuration.kafka.docs.consumer.batch;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public class Book {

    private String title;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}

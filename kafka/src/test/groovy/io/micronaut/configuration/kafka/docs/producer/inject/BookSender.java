/*
 * Copyright 2017-2019 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.kafka.docs.producer.inject;

import io.micronaut.configuration.kafka.docs.consumer.batch.Book;
// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import org.apache.kafka.clients.producer.*;

import javax.inject.Singleton;
import java.util.concurrent.Future;
// end::imports[]

// tag::clazz[]
@Singleton
public class BookSender {

    private final Producer<String, Book> kafkaProducer;

    public BookSender(
            @KafkaClient("book-producer") Producer<String, Book> kafkaProducer) { // <1>
        this.kafkaProducer = kafkaProducer;
    }

    public Future<RecordMetadata> send(String author, Book book) {
        return kafkaProducer.send(new ProducerRecord<>("books", author, book)); // <2>
    }

}
// end::clazz[]

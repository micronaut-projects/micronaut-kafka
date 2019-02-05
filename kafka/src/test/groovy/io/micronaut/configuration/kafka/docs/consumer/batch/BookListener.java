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
package io.micronaut.configuration.kafka.docs.consumer.batch;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.List;
// end::imports[]

// tag::clazz[]
@KafkaListener(batch = true) // <1>
public class BookListener {
// end::clazz[]

    // tag::method[]
    @Topic("all-the-books")
    public void receiveList(List<Book> books) { // <2>
        for (Book book : books) {
            System.out.println("Got Book = " + book.getTitle()); // <3>
        }
    }
    // end::method[]


    // tag::reactive[]
    @Topic("all-the-books")
    public Flux<Book> receiveFlux(Flux<Book> books) {
        return books.doOnNext(book ->
                System.out.println("Got Book = " + book.getTitle())
        );
    }
    // end::reactive[]

    // tag::manual[]
    @Topic("all-the-books")
    public void receive(
            List<Book> books,
            List<Long> offsets,
            List<Integer> partitions,
            List<String> topics,
            KafkaConsumer kafkaConsumer) { // <1>
        for (int i = 0; i < books.size(); i++) {

            // process the book
            Book book = books.get(i); // <2>

            // commit offsets
            String topic = topics.get(i);
            int partition = partitions.get(i);
            long offset = offsets.get(i); // <3>

            kafkaConsumer.commitSync(Collections.singletonMap( // <4>
                    new TopicPartition(topic, partition),
                    new OffsetAndMetadata(offset + 1, "my metadata")
            ));

        }
    }
    // end::manual[]

}

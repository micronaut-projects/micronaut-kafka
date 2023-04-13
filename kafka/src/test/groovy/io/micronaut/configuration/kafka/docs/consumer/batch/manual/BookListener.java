package io.micronaut.configuration.kafka.docs.consumer.batch.manual;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.configuration.kafka.docs.consumer.batch.Book;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.List;

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST;
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.DISABLED;
// end::imports[]

class BookListener {

    // tag::method[]
    @KafkaListener(offsetReset = EARLIEST, offsetStrategy = DISABLED, batch = true) // <1>
    @Topic("all-the-books")
    public void receive(List<Book> books,
                        List<Long> offsets,
                        List<Integer> partitions,
                        List<String> topics,
                        Consumer kafkaConsumer) { // <2>

        for (int i = 0; i < books.size(); i++) {

            // process the book
            Book book = books.get(i); // <3>

            // commit offsets
            String topic = topics.get(i);
            int partition = partitions.get(i);
            long offset = offsets.get(i); // <4>

            kafkaConsumer.commitSync(Collections.singletonMap( // <5>
                new TopicPartition(topic, partition),
                new OffsetAndMetadata(offset + 1, "my metadata")
            ));

        }
    }
    // end::method[]
}

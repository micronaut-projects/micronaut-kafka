package io.micronaut.kafka.docs.consumer.batch.manual;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;
import io.micronaut.kafka.docs.consumer.batch.Book;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.List;

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST;
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.DISABLED;

@Requires(property = "spec.name", value = "BatchManualAckSpec")
class BookListener {

    // tag::method[]
    @KafkaListener(offsetReset = EARLIEST, offsetStrategy = DISABLED, batch = true) // <1>
    @Topic("all-the-books")
    public void receive(List<ConsumerRecord<String, Book>> records, Consumer kafkaConsumer) { // <2>

        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String, Book> record = records.get(i); // <3>

            // process the book
            Book book = record.value();

            // commit offsets
            String topic = record.topic();
            int partition = record.partition();
            long offset = record.offset(); // <4>

            kafkaConsumer.commitSync(Collections.singletonMap( // <5>
                new TopicPartition(topic, partition),
                new OffsetAndMetadata(offset + 1, "my metadata")
            ));

        }
    }
    // end::method[]
}

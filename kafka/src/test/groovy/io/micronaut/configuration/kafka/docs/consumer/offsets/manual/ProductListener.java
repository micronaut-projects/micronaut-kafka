package io.micronaut.configuration.kafka.docs.consumer.offsets.manual;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.configuration.kafka.docs.consumer.config.Product;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST;
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.DISABLED;
// end::imports[]

class ProductListener {

    // tag::method[]
    @KafkaListener(offsetReset = EARLIEST, offsetStrategy = DISABLED) // <1>
    @Topic("awesome-products")
    void receive(Product product,
                 long offset,
                 int partition,
                 String topic,
                 Consumer kafkaConsumer) { // <2>
        // process product record

        // commit offsets
        kafkaConsumer.commitSync(Collections.singletonMap( // <3>
                new TopicPartition(topic, partition),
                new OffsetAndMetadata(offset + 1, "my metadata")
        ));
    }
    // end::method[]
}

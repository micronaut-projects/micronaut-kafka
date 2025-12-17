package io.micronaut.kafka.docs.seek.ops;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.configuration.kafka.seek.KafkaSeekOperation;
import io.micronaut.configuration.kafka.seek.KafkaSeekOperations;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.kafka.docs.Product;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;

@KafkaListener(
    groupId = "kafka-seek-operations-group",
    offsetReset = OffsetReset.EARLIEST,
    properties = @Property(name = "max.poll.records", value = "1")
)
@Requires(property = "spec.name", value = "KafkaSeekOperationsTest")
public class ProductListener {

    List<Product> processed = new ArrayList<>();

    public ProductListener(ProductListenerConfiguration config) {
        // ...
    }

    @Topic("amazing-products")
    void receive(Product product, KafkaSeekOperations ops) { // <1>
        processed.add(product);
        ops.defer(KafkaSeekOperation.seekToEnd(new TopicPartition("amazing-products", 0))); // <2>
    }
}

package io.micronaut.kafka.docs.seek.rebalance;

import io.micronaut.configuration.kafka.ConsumerAware;
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.context.annotation.Requires;
import io.micronaut.kafka.docs.Product;
import io.micronaut.core.annotation.NonNull;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import java.util.*;

@KafkaListener(offsetReset = OffsetReset.EARLIEST)
@Requires(property = "spec.name", value = "ConsumerRebalanceListenerTest")
public class ProductListener implements ConsumerRebalanceListener, ConsumerAware {

    List<Product> processed = new ArrayList<>();
    private Consumer consumer;

    public ProductListener(ProductListenerConfiguration config) {
        // ...
    }

    @Override
    public void setKafkaConsumer(@NonNull Consumer consumer) { // <1>
        this.consumer = consumer;
    }

    @Topic("awesome-products")
    void receive(Product product) {
        processed.add(product);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) { // <2>
        // save offsets here
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) { // <3>
        // seek to offset here
        for (TopicPartition partition : partitions) {
            consumer.seek(partition, 1);
        }
    }
}

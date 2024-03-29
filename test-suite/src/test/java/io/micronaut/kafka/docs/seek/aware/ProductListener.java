package io.micronaut.kafka.docs.seek.aware;

import io.micronaut.configuration.kafka.ConsumerSeekAware;
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.configuration.kafka.seek.*;
import io.micronaut.context.annotation.Requires;
import io.micronaut.kafka.docs.Product;
import org.apache.kafka.common.TopicPartition;
import java.util.*;

@KafkaListener
@Requires(property = "spec.name", value = "ConsumerSeekAwareTest")
public class ProductListener implements ConsumerSeekAware { // <1>

    List<Product> processed = new ArrayList<>();

    public ProductListener(ProductListenerConfiguration config) {
        // ...
    }

    @Topic("wonderful-products")
    void receive(Product product) {
        processed.add(product);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) { // <2>
        // save offsets here
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions, KafkaSeeker seeker) { // <3>
        // seek to offset here
        partitions.stream().map(tp -> KafkaSeekOperation.seek(tp, 1)).forEach(seeker::perform);
    }
}

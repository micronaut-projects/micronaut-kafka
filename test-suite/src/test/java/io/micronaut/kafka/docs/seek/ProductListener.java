package io.micronaut.kafka.docs.seek;

import io.micronaut.configuration.kafka.ConsumerSeekAware;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.configuration.kafka.seek.KafkaSeeker;
import io.micronaut.kafka.docs.Product;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

@KafkaListener
public class ProductListener implements ConsumerSeekAware { // <1>

    @Topic("awesome-products")
    void receive(Product product) {
        // process product
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) { // <2>
        // save offsets here
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions, KafkaSeeker seeker) { // <3>
        // seek to offset here
        partitions.stream().map(tp -> seeker.seek(tp, 1)).forEach(seeker::perform);
    }
}

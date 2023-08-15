package io.micronaut.kafka.docs.rebalance;

import io.micronaut.configuration.kafka.ConsumerAware;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.kafka.docs.Product;
import io.micronaut.core.annotation.NonNull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import java.util.Collection;

@KafkaListener
public class ProductListener implements ConsumerRebalanceListener, ConsumerAware {

    private Consumer consumer;

    @Override
    public void setKafkaConsumer(@NonNull Consumer consumer) { // <1>
        this.consumer = consumer;
    }

    @Topic("awesome-products")
    void receive(Product product) {
        // process product
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

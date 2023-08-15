package io.micronaut.kafka.docs.rebalance

import io.micronaut.configuration.kafka.ConsumerAware
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.core.annotation.NonNull
import io.micronaut.kafka.docs.Product
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

@KafkaListener
class ProductListener implements ConsumerRebalanceListener, ConsumerAware {

    private Consumer consumer

    @Override
    void setKafkaConsumer(@NonNull Consumer consumer) { // <1>
        this.consumer = consumer
    }

    @Topic("awesome-products")
    def receive(Product product) {
        // process product
    }

    @Override
    void onPartitionsRevoked(Collection<TopicPartition> partitions) { // <2>
        // save offsets here
    }

    @Override
    void onPartitionsAssigned(Collection<TopicPartition> partitions) { // <3>
        // seek to offset here
        for (TopicPartition partition : partitions) {
            consumer.seek(partition, 1)
        }
    }
}

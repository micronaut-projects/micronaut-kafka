package io.micronaut.kafka.docs.rebalance

import io.micronaut.configuration.kafka.ConsumerAware
import io.micronaut.configuration.kafka.annotation.*
import io.micronaut.context.annotation.Requires
import io.micronaut.core.annotation.NonNull
import io.micronaut.kafka.docs.Product
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition

@KafkaListener(offsetReset = OffsetReset.EARLIEST)
@Requires(property = "spec.name", value = "ConsumerRebalanceListenerSpec")
class ProductListener implements ConsumerRebalanceListener, ConsumerAware {

    List<Product> processed = []
    private Consumer consumer

    @Override
    void setKafkaConsumer(@NonNull Consumer consumer) { // <1>
        this.consumer = consumer
    }

    @Topic("awesome-products")
    void receive(Product product) {
        processed << product
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

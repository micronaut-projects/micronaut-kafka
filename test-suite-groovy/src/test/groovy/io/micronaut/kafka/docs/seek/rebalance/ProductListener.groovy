package io.micronaut.kafka.docs.seek.rebalance

import io.micronaut.configuration.kafka.ConsumerAware
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.core.annotation.NonNull
import io.micronaut.kafka.docs.Product
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

@KafkaListener(groupId = 'consumer-rebalance-listener-group', offsetReset = OffsetReset.EARLIEST)
@Requires(property = "spec.name", value = "ConsumerRebalanceListenerSpec")
class ProductListener implements ConsumerRebalanceListener, ConsumerAware {

    List<Product> processed = []
    private Consumer consumer

    ProductListener(ProductListenerConfiguration config) {
        // ...
    }

    @Override
    void setKafkaConsumer(@NonNull Consumer consumer) { // <1>
        this.consumer = consumer
    }

    @Topic("fantastic-products")
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

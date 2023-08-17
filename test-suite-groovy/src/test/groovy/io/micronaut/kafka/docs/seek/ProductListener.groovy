package io.micronaut.kafka.docs.seek

import io.micronaut.configuration.kafka.ConsumerSeekAware
import io.micronaut.configuration.kafka.annotation.*
import io.micronaut.configuration.kafka.seek.KafkaSeeker
import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.Product
import org.apache.kafka.common.TopicPartition

@KafkaListener
@Requires(property = "spec.name", value = "ConsumerSeekAwareSpec")
class ProductListener implements ConsumerSeekAware { // <1>

    List<Product> processed = []

    @Topic("awesome-products")
    void receive(Product product) {
        processed << product
    }

    @Override
    void onPartitionsRevoked(Collection<TopicPartition> partitions) { // <2>
        // save offsets here
    }

    @Override
    void onPartitionsAssigned(Collection<TopicPartition> partitions, KafkaSeeker seeker) { // <3>
        // seek to offset here
        partitions.collect {seeker.seek(it, 1) }.each(seeker.&perform)
    }
}

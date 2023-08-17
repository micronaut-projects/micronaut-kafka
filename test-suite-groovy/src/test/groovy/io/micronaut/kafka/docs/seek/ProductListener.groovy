package io.micronaut.kafka.docs.seek

import io.micronaut.configuration.kafka.ConsumerSeekAware
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.seek.KafkaSeeker
import io.micronaut.kafka.docs.Product
import org.apache.kafka.common.TopicPartition

@KafkaListener
class ProductListener implements ConsumerSeekAware { // <1>

    @Topic("awesome-products")
    void receive(Product product) {
        // process product
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

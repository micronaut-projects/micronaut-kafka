package io.micronaut.kafka.docs.seek

import io.micronaut.configuration.kafka.ConsumerSeekAware
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.seek.KafkaSeeker
import io.micronaut.kafka.docs.Product
import org.apache.kafka.common.TopicPartition

@KafkaListener
class ProductListener : ConsumerSeekAware { // <1>

    @Topic("awesome-products")
    fun receive(product: Product?) {
        // process product
    }

    override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) { // <2>
        // save offsets here
    }

    override fun onPartitionsAssigned(partitions: Collection<TopicPartition>, seeker: KafkaSeeker) { // <3>
        // seek to offset here
        partitions.stream().map { tp -> seeker.seek(tp, 1) }.forEach(seeker::perform)
    }
}

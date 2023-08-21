package io.micronaut.kafka.docs.seek.aware

import io.micronaut.configuration.kafka.ConsumerSeekAware
import io.micronaut.configuration.kafka.annotation.*
import io.micronaut.configuration.kafka.seek.*
import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.Product
import jakarta.inject.Inject
import org.apache.kafka.common.TopicPartition

@KafkaListener
@Requires(property = "spec.name", value = "ConsumerSeekAwareTest")
class ProductListener @Inject constructor(config: ProductListenerConfiguration) : ConsumerSeekAware { // <1>

    var processed: MutableList<Product> = mutableListOf()

    @Topic("wonderful-products")
    fun receive(product: Product) {
        processed.add(product)
    }

    override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) { // <2>
        // save offsets here
    }

    override fun onPartitionsAssigned(partitions: Collection<TopicPartition>, seeker: KafkaSeeker) { // <3>
        // seek to offset here
        partitions.stream().map { tp -> KafkaSeekOperation.seek(tp, 1) }.forEach(seeker::perform)
    }
}

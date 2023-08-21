package io.micronaut.kafka.docs.seek.rebalance

import io.micronaut.configuration.kafka.ConsumerAware
import io.micronaut.configuration.kafka.annotation.*
import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.Product
import jakarta.inject.Inject
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition

@KafkaListener(offsetReset = OffsetReset.EARLIEST)
@Requires(property = "spec.name", value = "ConsumerRebalanceListenerTest")
class ProductListener @Inject constructor(config: ProductListenerConfiguration) : ConsumerRebalanceListener, ConsumerAware<Any?, Any?> {

    var processed: MutableList<Product> = mutableListOf()
    private var consumer: Consumer<*, *>? = null

    override fun setKafkaConsumer(consumer: Consumer<Any?, Any?>?) { // <1>
        this.consumer = consumer
    }

    @Topic("awesome-products")
    fun receive(product: Product) {
        processed.add(product)
    }

    override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) { // <2>
        // save offsets here
    }

    override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) { // <3>
        // seek to offset here
        for (partition in partitions) {
            consumer!!.seek(partition, 1)
        }
    }
}

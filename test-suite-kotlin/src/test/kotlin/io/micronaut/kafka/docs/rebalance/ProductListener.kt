package io.micronaut.kafka.docs.rebalance

import io.micronaut.configuration.kafka.ConsumerAware
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.kafka.docs.Product
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

@KafkaListener
class ProductListener : ConsumerRebalanceListener, ConsumerAware<Any?, Any?> {

    private var consumer: Consumer<*, *>? = null

    override fun setKafkaConsumer(consumer: Consumer<Any?, Any?>?) { // <1>
        this.consumer = consumer
    }

    @Topic("awesome-products")
    fun receive(product: Product?) {
        // process product
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

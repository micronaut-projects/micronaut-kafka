package io.micronaut.kafka.docs.seek.ops

import io.micronaut.configuration.kafka.annotation.*
import io.micronaut.configuration.kafka.seek.KafkaSeekOperations
import io.micronaut.context.annotation.*
import io.micronaut.kafka.docs.Product
import org.apache.kafka.common.TopicPartition

@KafkaListener(offsetReset = OffsetReset.EARLIEST, properties = [Property(name = "max.poll.records", value = "1")])
@Requires(property = "spec.name", value = "KafkaSeekOperationsTest")
class ProductListener {

    var processed: MutableList<Product> = mutableListOf()

    @Topic("awesome-products")
    fun receive(product: Product, ops: KafkaSeekOperations) { // <1>
        processed.add(product)
        ops.defer(ops.seekToBeginning(TopicPartition("awesome-products", 0))) // <2>
    }
}

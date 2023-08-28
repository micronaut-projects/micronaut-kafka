package io.micronaut.kafka.docs.seek.ops

import io.micronaut.configuration.kafka.annotation.*
import io.micronaut.configuration.kafka.seek.*
import io.micronaut.context.annotation.*
import io.micronaut.kafka.docs.Product
import org.apache.kafka.common.TopicPartition

@KafkaListener(offsetReset = OffsetReset.EARLIEST, properties = [Property(name = "max.poll.records", value = "1")])
@Requires(property = "spec.name", value = "KafkaSeekOperationsTest")
class ProductListener constructor(config: ProductListenerConfiguration) {

    var processed: MutableList<Product> = mutableListOf()

    @Topic("amazing-products")
    fun receive(product: Product, ops: KafkaSeekOperations) { // <1>
        processed.add(product)
        ops.defer(KafkaSeekOperation.seekToBeginning(TopicPartition("amazing-products", 0))) // <2>
    }
}

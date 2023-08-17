package io.micronaut.kafka.docs.seek.ops

import io.micronaut.configuration.kafka.annotation.*
import io.micronaut.configuration.kafka.seek.KafkaSeekOperations
import io.micronaut.context.annotation.*
import io.micronaut.kafka.docs.Product
import org.apache.kafka.common.TopicPartition

@KafkaListener(offsetReset = OffsetReset.EARLIEST, properties = @Property(name = "max.poll.records", value = "1"))
@Requires(property = "spec.name", value = "KafkaSeekOperationsSpec")
class ProductListener {

    List<Product> processed = []

    @Topic("awesome-products")
    void receive(Product product, KafkaSeekOperations ops) { // <1>
        processed << product
        ops.defer(ops.seekToEnd(new TopicPartition("awesome-products", 0))); // <2>
    }
}

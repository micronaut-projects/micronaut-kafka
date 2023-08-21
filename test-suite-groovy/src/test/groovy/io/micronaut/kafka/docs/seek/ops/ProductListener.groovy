package io.micronaut.kafka.docs.seek.ops

import io.micronaut.configuration.kafka.annotation.*
import io.micronaut.configuration.kafka.seek.*
import io.micronaut.context.annotation.*
import io.micronaut.kafka.docs.Product
import org.apache.kafka.common.TopicPartition

@KafkaListener(offsetReset = OffsetReset.EARLIEST, properties = @Property(name = "max.poll.records", value = "1"))
@Requires(property = "spec.name", value = "KafkaSeekOperationsSpec")
class ProductListener {

    List<Product> processed = []

    ProductListener(ProductListenerConfiguration config) {
        // ...
    }

    @Topic("amazing-products")
    void receive(Product product, KafkaSeekOperations ops) { // <1>
        processed << product
        ops.defer(KafkaSeekOperation.seekToEnd(new TopicPartition("amazing-products", 0))); // <2>
    }
}

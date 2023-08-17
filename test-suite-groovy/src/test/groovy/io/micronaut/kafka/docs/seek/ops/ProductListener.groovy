package io.micronaut.kafka.docs.seek.ops

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.seek.KafkaSeekOperations
import io.micronaut.kafka.docs.Product
import org.apache.kafka.common.TopicPartition

@KafkaListener
class ProductListener {

    @Topic("awesome-products")
    void receive(Product product, KafkaSeekOperations ops) { // <1>
        // process product
        ops.defer(ops.seekToBeginning(new TopicPartition("awesome-products", 0))); // <2>
    }
}

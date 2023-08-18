package io.micronaut.kafka.docs.seek.ops;

import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.configuration.kafka.seek.*;
import io.micronaut.context.annotation.*;
import io.micronaut.kafka.docs.Product;
import org.apache.kafka.common.TopicPartition;
import java.util.*;

@KafkaListener(offsetReset = OffsetReset.EARLIEST, properties = @Property(name = "max.poll.records", value = "1"))
@Requires(property = "spec.name", value = "KafkaSeekOperationsTest")
public class ProductListener {

    List<Product> processed = new ArrayList<>();

    @Topic("awesome-products")
    void receive(Product product, KafkaSeekOperations ops) { // <1>
        processed.add(product);
        ops.defer(KafkaSeekOperation.seekToEnd(new TopicPartition("awesome-products", 0))); // <2>
    }
}

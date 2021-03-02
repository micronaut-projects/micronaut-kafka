package io.micronaut.configuration.kafka.docs.quickstart;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
// end::imports[]

// tag::clazz[]
@KafkaListener(offsetReset = OffsetReset.EARLIEST) // <1>
public class ProductListener {

    @Topic("my-products") // <2>
    public void receive(@KafkaKey String brand, String name) { // <3>
        System.out.println("Got Product - " + name + " by " + brand);
    }
}
// end::clazz[]

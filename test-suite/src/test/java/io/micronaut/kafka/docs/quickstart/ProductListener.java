package io.micronaut.kafka.docs.quickstart;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;
// end::imports[]

@Requires(property = "spec.name", value = "QuickstartTest")
// tag::clazz[]
@KafkaListener(offsetReset = OffsetReset.EARLIEST) // <1>
public class ProductListener {
    private static final Logger LOG = getLogger(ProductListener.class);

    @Topic("my-products") // <2>
    public void receive(@KafkaKey String brand, String name) { // <3>
        LOG.info("Got Product - {} by {}", name, brand);
    }
}
// end::clazz[]

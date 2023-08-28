package io.micronaut.kafka.docs.consumer.sendto;

import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

@Requires(property = "spec.name", value = "SendToProductListenerTest")
@KafkaListener(offsetReset = OffsetReset.EARLIEST)
public class QuantityListener {

    private static final Logger LOG = getLogger(QuantityListener.class);

    Integer quantity;

    @Topic("product-quantities")
    public void receive(@KafkaKey String brand, Integer quantity) {
        LOG.info("Got quantity - {} by {}", quantity, brand);
        this.quantity = quantity;
    }
}

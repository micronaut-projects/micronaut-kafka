package io.micronaut.kafka.docs.consumer.config;

// tag::imports[]

import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.kafka.docs.Product;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;
// end::imports[]

@Requires(property = "spec.name", value = "ConfigProductListenerTest")
// tag::clazz[]
@KafkaListener(
    groupId = "products",
    pollTimeout = "500ms",
    properties = @Property(name = ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, value = "10000")
)
public class ProductListener {
// end::clazz[]

    private static final Logger LOG = getLogger(ProductListener.class);

    // tag::method[]
    @Topic("awesome-products")
    public void receive(@KafkaKey String brand, // <1>
                        Product product, // <2>
                        long offset, // <3>
                        int partition, // <4>
                        String topic, // <5>
                        long timestamp) { // <6>
        LOG.info("Got Product - {} by {}",product.name(), brand);
    }
    // end::method[]

    // tag::consumeRecord[]
    @Topic("awesome-products")
    public void receive(ConsumerRecord<String, Product> record) { // <1>
        Product product = record.value(); // <2>
        String brand = record.key(); // <3>
        LOG.info("Got Product - {} by {}",product.name(), brand);
    }
    // end::consumeRecord[]
}

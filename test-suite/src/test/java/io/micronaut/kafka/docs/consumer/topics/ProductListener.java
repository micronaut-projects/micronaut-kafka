package io.micronaut.kafka.docs.consumer.topics;

import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

@Requires(property = "spec.name", value = "TopicsProductListenerTest")
@KafkaListener
public class ProductListener {
    private static final Logger LOG = getLogger(ProductListener.class);

    // tag::multiTopics[]
    @Topic({"fun-products", "awesome-products"})
    public void receiveMultiTopics(@KafkaKey String brand, String name) {
        LOG.info("Got Product - {} by {}", name, brand);
    }
    // tag::multiTopics[]

    // tag::patternTopics[]
    @Topic(patterns="products-\\w+")
    public void receivePatternTopics(@KafkaKey String brand, String name) {
        LOG.info("Got Product - {} by {}", name, brand);
    }
    // tag::patternTopics[]
}

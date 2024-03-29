package io.micronaut.kafka.docs.consumer.sendto;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;
import io.micronaut.kafka.docs.Product;
import io.micronaut.kafka.docs.consumer.batch.BookListener;
import io.micronaut.messaging.annotation.SendTo;
import org.slf4j.Logger;
import reactor.core.publisher.Mono;

import static org.slf4j.LoggerFactory.getLogger;
// end::imports[]

@Requires(property = "spec.name", value = "SendToProductListenerTest")
@KafkaListener(offsetReset = OffsetReset.EARLIEST)
public class ProductListener {
    private static final Logger LOG = getLogger(BookListener.class);

    // tag::method[]
    @Topic("sendto-products") // <1>
    @SendTo("product-quantities") // <2>
    public int receive(@KafkaKey String brand, Product product) {
        LOG.info("Got Product - {} by {}", product.name(), brand);
        return product.quantity(); // <3>
    }
    // end::method[]

    // tag::reactive[]
    @Topic("sendto-products") // <1>
    @SendTo("product-quantities") // <2>
    public Mono<Integer> receiveProduct(@KafkaKey String brand,
                                        Mono<Product> productSingle) {

        return productSingle.map(product -> {
            LOG.info("Got Product - {} by {}", product.name(), brand);
            return product.quantity(); // <3>
        });
    }
    // end::reactive[]
}

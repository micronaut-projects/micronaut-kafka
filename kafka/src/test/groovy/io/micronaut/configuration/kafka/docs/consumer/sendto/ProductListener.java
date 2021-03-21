package io.micronaut.configuration.kafka.docs.consumer.sendto;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.configuration.kafka.docs.consumer.config.Product;
import io.micronaut.messaging.annotation.SendTo;
import io.reactivex.Single;
// end::imports[]

@KafkaListener
public class ProductListener {

    // tag::method[]
    @Topic("awesome-products") // <1>
    @SendTo("product-quantities") // <2>
    public int receive(@KafkaKey String brand,
                       Product product) {
        System.out.println("Got Product - " + product.getName() + " by " + brand);

        return product.getQuantity(); // <3>
    }
    // end::method[]

    // tag::reactive[]
    @Topic("awesome-products") // <1>
    @SendTo("product-quantities") // <2>
    public Single<Integer> receiveProduct(@KafkaKey String brand,
                                          Single<Product> productSingle) {

        return productSingle.map(product -> {
            System.out.println("Got Product - " + product.getName() + " by " + brand);
            return product.getQuantity(); // <3>
        });
    }
    // end::reactive[]
}

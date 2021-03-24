package io.micronaut.configuration.kafka.annotation

import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.context.annotation.Requires
import io.micronaut.messaging.annotation.SendTo
import io.reactivex.Flowable
import io.reactivex.Single
import org.apache.kafka.clients.producer.RecordMetadata
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import spock.lang.Unroll

import javax.annotation.Nullable
import java.util.concurrent.ConcurrentLinkedDeque

import static io.micronaut.configuration.kafka.annotation.KafkaClient.Acknowledge.ALL
import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

@Unroll
class KafkaSendToSpec extends AbstractKafkaContainerSpec {

    public static final String TOPIC_SINGLE = "KafkaSendToSpec-products-single"
    public static final String TOPIC_BLOCKING = "KafkaSendToSpec-products-blocking"
    public static final String TOPIC_FLOWABLE = "KafkaSendToSpec-products-flowable"
    public static final String TOPIC_FLUX = "KafkaSendToSpec-products-flux"
    public static final String TOPIC_MONO = "KafkaSendToSpec-products-mono"
    public static final String TOPIC_QUANTITY = "KafkaSendToSpec-quantity"

    protected Map<String, Object> getConfiguration() {
        super.configuration +
                [(EMBEDDED_TOPICS): [TOPIC_SINGLE, TOPIC_QUANTITY, TOPIC_FLOWABLE, TOPIC_FLUX, TOPIC_MONO]]
    }

    void "test send to another topic - blocking"() {
        given:
        ProductClient client = context.getBean(ProductClient)
        ProductListener productListener = context.getBean(ProductListener)
        QuantityListener quantityListener = context.getBean(QuantityListener)
        productListener.products.clear()
        quantityListener.quantities.clear()

        when:
        client.sendProductBlocking(key, message)

        then:
        conditions.eventually {
            productListener.products.size() == 1
            productListener.products.iterator().next().name == key
            quantityListener.quantities.size() == 1
            quantityListener.quantities.iterator().next() == quantity
        }

        where:
        message                                 | key    | quantity
        new Product(name: "Apple", quantity: 5) | "Apple" | 5
        null                                    | "null"  | 0
    }

    void "test send to another topic - single"() {
        given:
        ProductClient client = context.getBean(ProductClient)
        ProductListener productListener = context.getBean(ProductListener)
        QuantityListener quantityListener = context.getBean(QuantityListener)
        productListener.products.clear()
        quantityListener.quantities.clear()

        when:
        client.sendProduct(key, message)

        then:
        conditions.eventually {
            productListener.products.size() == 1
            productListener.products.iterator().next().name == key
            quantityListener.quantities.size() == 1
            quantityListener.quantities.iterator().next() == quantity
        }

        where:
        message                                 | key     | quantity
        new Product(name: "Apple", quantity: 5) | "Apple" | 5
        null                                    | "null"  | 0
    }

    void "test send to another topic - flowable"() {
        given:
        ProductClient client = context.getBean(ProductClient)
        ProductListener productListener = context.getBean(ProductListener)
        QuantityListener quantityListener = context.getBean(QuantityListener)
        productListener.products.clear()
        quantityListener.quantities.clear()

        when:
        client.sendProductForFlowable(key, message)

        then:
        conditions.eventually {
            productListener.products.size() == 1
            productListener.products.iterator().next().name == key
            quantityListener.quantities.size() == 1
            quantityListener.quantities.iterator().next() == quantity
        }

        where:
        message                                 | key     | quantity
        new Product(name: "Apple", quantity: 5) | "Apple" | 5
        null                                    | "null"  | 0
    }

    void "test send to another topic - flux"() {
        given:
        ProductClient client = context.getBean(ProductClient)
        ProductListener productListener = context.getBean(ProductListener)
        QuantityListener quantityListener = context.getBean(QuantityListener)
        productListener.products.clear()
        quantityListener.quantities.clear()

        when:
        client.sendProductForFlux(key, message)

        then:
        conditions.eventually {
            productListener.products.size() == 1
            productListener.products.iterator().next().name == key
            quantityListener.quantities.size() == 1
            quantityListener.quantities.iterator().next() == quantity
        }

        where:
        message                                 | key     | quantity
        new Product(name: "Apple", quantity: 5) | "Apple" | 5
        null                                    | "null"  | 0
    }

    void "test send to another topic - mono"() {
        given:
        ProductClient client = context.getBean(ProductClient)
        ProductListener productListener = context.getBean(ProductListener)
        QuantityListener quantityListener = context.getBean(QuantityListener)
        productListener.products.clear()
        quantityListener.quantities.clear()

        when:
        client.sendProductForMono(key, message)

        then:
        conditions.eventually {
            productListener.products.size() == 1
            productListener.products.iterator().next().name == key
            quantityListener.quantities.size() == 1
            quantityListener.quantities.iterator().next() == quantity
        }

        where:
        message                                 | key     | quantity
        new Product(name: "Apple", quantity: 5) | "Apple" | 5
        null                                    | "null"  | 0
    }

    @Requires(property = 'spec.name', value = 'KafkaSendToSpec')
    @KafkaClient(acks = ALL)
    static interface ProductClient {
        @Topic(KafkaSendToSpec.TOPIC_SINGLE)
        RecordMetadata sendProduct(@KafkaKey String name, @Nullable Product product)

        @Topic(KafkaSendToSpec.TOPIC_BLOCKING)
        RecordMetadata sendProductBlocking(@KafkaKey String name, @Nullable Product product)

        @Topic(KafkaSendToSpec.TOPIC_FLOWABLE)
        RecordMetadata sendProductForFlowable(@KafkaKey String name, @Nullable Product product)

        @Topic(KafkaSendToSpec.TOPIC_FLUX)
        RecordMetadata sendProductForFlux(@KafkaKey String name, @Nullable Product product)

        @Topic(KafkaSendToSpec.TOPIC_MONO)
        RecordMetadata sendProductForMono(@KafkaKey String name, @Nullable Product product)
    }

    @Requires(property = 'spec.name', value = 'KafkaSendToSpec')
    @KafkaListener(offsetReset = EARLIEST)
    static class ProductListener {
        Queue<Product> products = new ConcurrentLinkedDeque<>()

        @Topic(KafkaSendToSpec.TOPIC_BLOCKING)
        @SendTo(KafkaSendToSpec.TOPIC_QUANTITY)
        Integer receive(@KafkaKey String name, @Nullable Product product) {
            if (product) {
                products << product
            } else {
                products << new Product(name: name, quantity: 0)
            }

            return product?.quantity ?: 0
        }

        @Topic(KafkaSendToSpec.TOPIC_SINGLE)
        @SendTo(KafkaSendToSpec.TOPIC_QUANTITY)
        Single<Integer> receiveSingle(@KafkaKey String name, @Nullable Single<Product> product) {
            if (product) {
                return product.map({ Product p ->
                    products << p
                    p.quantity
                })
            } else {
                products << new Product(name: name, quantity: 0)
                return Single.just(0)
            }
        }

        @Topic(KafkaSendToSpec.TOPIC_FLOWABLE)
        @SendTo(KafkaSendToSpec.TOPIC_QUANTITY)
        Flowable<Integer> receiveFlowable(@KafkaKey String name, @Nullable Flowable<Product> product) {
            if (product) {
                return product.map({ Product p ->
                    products << p
                    p.quantity
                })
            } else {
                products << new Product(name: name, quantity: 0)
                return Flowable.just(0)
            }
        }

        @Topic(KafkaSendToSpec.TOPIC_FLUX)
        @SendTo(KafkaSendToSpec.TOPIC_QUANTITY)
        Flux<Integer> receiveFlux(@KafkaKey String name, @Nullable Flux<Product> product) {
            if (product) {
                return product.map({ Product p ->
                    products << p
                    p.quantity
                })
            } else {
                products << new Product(name: name, quantity: 0)
                return Flux.just(0)
            }
        }

        @Topic(KafkaSendToSpec.TOPIC_MONO)
        @SendTo(KafkaSendToSpec.TOPIC_QUANTITY)
        Mono<Integer> receiveMono(@KafkaKey String name, @Nullable Mono<Product> product) {
            if (product) {
                return product.map({ Product p ->
                    products << p
                    p.quantity
                })
            } else {
                products << new Product(name: name, quantity: 0)
                return Mono.just(0)
            }
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaSendToSpec')
    @KafkaListener(offsetReset = EARLIEST)
    @Topic(KafkaSendToSpec.TOPIC_QUANTITY)
    static class QuantityListener {
        Queue<Integer> quantities = new ConcurrentLinkedDeque<>()

        void receiveQuantity(int q) {
            quantities.add(q)
        }
    }

    static class Product {
        String name
        int quantity
    }
}

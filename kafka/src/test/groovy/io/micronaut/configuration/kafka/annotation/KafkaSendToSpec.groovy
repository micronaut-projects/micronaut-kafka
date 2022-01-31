package io.micronaut.configuration.kafka.annotation

import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.context.annotation.Requires
import io.micronaut.messaging.annotation.SendTo
import io.reactivex.Flowable
import io.reactivex.Single
import org.apache.kafka.clients.producer.RecordMetadata
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

import java.util.concurrent.ConcurrentLinkedDeque

import static io.micronaut.configuration.kafka.annotation.KafkaClient.Acknowledge.ALL
import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

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
        client.sendProductBlocking("Apple", new Product(name: "Apple", quantity: 5))

        then:
        conditions.eventually {
            productListener.products.size() == 1
            productListener.products.iterator().next().name == "Apple"
            quantityListener.quantities.size() == 1
            quantityListener.quantities.iterator().next() == 5
        }
    }

    void "test send to another topic - single"() {
        given:
        ProductClient client = context.getBean(ProductClient)
        ProductListener productListener = context.getBean(ProductListener)
        QuantityListener quantityListener = context.getBean(QuantityListener)
        productListener.products.clear()
        quantityListener.quantities.clear()

        when:
        client.sendProduct("Apple", new Product(name: "Apple", quantity: 5))

        then:
        conditions.eventually {
            productListener.products.size() == 1
            productListener.products.iterator().next().name == "Apple"
            quantityListener.quantities.size() == 1
            quantityListener.quantities.iterator().next() == 5
        }
    }

    void "test send to another topic - flowable"() {
        given:
        ProductClient client = context.getBean(ProductClient)
        ProductListener productListener = context.getBean(ProductListener)
        QuantityListener quantityListener = context.getBean(QuantityListener)
        productListener.products.clear()
        quantityListener.quantities.clear()

        when:
        client.sendProductForFlowable("Apple", new Product(name: "Apple", quantity: 5))

        then:
        conditions.eventually {
            productListener.products.size() == 1
            productListener.products.iterator().next().name == "Apple"
            quantityListener.quantities.size() == 1
            quantityListener.quantities.iterator().next() == 5
        }
    }

    void "test send to another topic - flux"() {
        given:
        ProductClient client = context.getBean(ProductClient)
        ProductListener productListener = context.getBean(ProductListener)
        QuantityListener quantityListener = context.getBean(QuantityListener)
        productListener.products.clear()
        quantityListener.quantities.clear()

        when:
        client.sendProductForFlux("Apple", new Product(name: "Apple", quantity: 5))

        then:
        conditions.eventually {
            productListener.products.size() == 1
            productListener.products.iterator().next().name == "Apple"
            quantityListener.quantities.size() == 1
            quantityListener.quantities.iterator().next() == 5
        }
    }

    void "test send to another topic - mono"() {
        given:
        ProductClient client = context.getBean(ProductClient)
        ProductListener productListener = context.getBean(ProductListener)
        QuantityListener quantityListener = context.getBean(QuantityListener)
        productListener.products.clear()
        quantityListener.quantities.clear()

        when:
        client.sendProductForMono("Apple", new Product(name: "Apple", quantity: 5))

        then:
        conditions.eventually {
            productListener.products.size() == 1
            productListener.products.iterator().next().name == "Apple"
            quantityListener.quantities.size() == 1
            quantityListener.quantities.iterator().next() == 5
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaSendToSpec')
    @KafkaClient(acks = ALL)
    static interface ProductClient {
        @Topic(KafkaSendToSpec.TOPIC_SINGLE)
        RecordMetadata sendProduct(@KafkaKey String name, Product product)

        @Topic(KafkaSendToSpec.TOPIC_BLOCKING)
        RecordMetadata sendProductBlocking(@KafkaKey String name, Product product)

        @Topic(KafkaSendToSpec.TOPIC_FLOWABLE)
        RecordMetadata sendProductForFlowable(@KafkaKey String name, Product product)

        @Topic(KafkaSendToSpec.TOPIC_FLUX)
        RecordMetadata sendProductForFlux(@KafkaKey String name, Product product)

        @Topic(KafkaSendToSpec.TOPIC_MONO)
        RecordMetadata sendProductForMono(@KafkaKey String name, Product product)
    }

    @Requires(property = 'spec.name', value = 'KafkaSendToSpec')
    @KafkaListener(offsetReset = EARLIEST)
    static class ProductListener {
        Queue<Product> products = new ConcurrentLinkedDeque<>()

        @Topic(KafkaSendToSpec.TOPIC_BLOCKING)
        @SendTo(KafkaSendToSpec.TOPIC_QUANTITY)
        Integer receive(Product product) {
            products << product
            return product.quantity
        }

        @Topic(KafkaSendToSpec.TOPIC_SINGLE)
        @SendTo(KafkaSendToSpec.TOPIC_QUANTITY)
        Single<Integer> receiveSingle(Single<Product> product) {
            product.map({ Product p ->
                products << p
                p.quantity
            })
        }

        @Topic(KafkaSendToSpec.TOPIC_FLOWABLE)
        @SendTo(KafkaSendToSpec.TOPIC_QUANTITY)
        Flowable<Integer> receiveFlowable(Flowable<Product> product) {
            product.map{ Product p ->
                products << p
                return p.quantity
            }
        }

        @Topic(KafkaSendToSpec.TOPIC_FLUX)
        @SendTo(KafkaSendToSpec.TOPIC_QUANTITY)
        Flux<Integer> receiveFlux(Flux<Product> product) {
            product.map{ Product p ->
                products << p
                return p.quantity
            }
        }

        @Topic(KafkaSendToSpec.TOPIC_MONO)
        @SendTo(KafkaSendToSpec.TOPIC_QUANTITY)
        Mono<Integer> receiveMono(Mono<Product> product) {
            product.map{ Product p ->
                products << p
                return p.quantity
            }
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaSendToSpec')
    @KafkaListener(offsetReset = EARLIEST)
    @Topic(KafkaSendToSpec.TOPIC_QUANTITY)
    static class QuantityListener {
        Queue<Integer> quantities = new ConcurrentLinkedDeque<>()

        void receiveQuantity(int q) {
            quantities << q
        }
    }

    static class Product {
        String name
        int quantity
    }
}

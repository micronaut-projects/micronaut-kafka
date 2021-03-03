package io.micronaut.configuration.kafka.offsets

import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import javax.inject.Singleton

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.DISABLED
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

class ManualOffsetCommitSpec extends AbstractKafkaContainerSpec {

    public static final String TOPIC_SYNC = "ManualOffsetCommitSpec-products-sync"

    protected Map<String, Object> getConfiguration() {
        super.configuration +
                [(EMBEDDED_TOPICS): [TOPIC_SYNC]]
    }

    void "test manual offset commit"() {
        given:
        ProductClient client = context.getBean(ProductClient)
        ProductListener listener = context.getBean(ProductListener)

        when:
        client.send(new Product(name: "Apple"))
        client.send(new Product(name: "Orange"))

        then:
        conditions.eventually {
            listener.products.size() == 2
            listener.products.find() { it.name == "Apple"}
        }
    }

    @Requires(property = 'spec.name', value = 'ManualOffsetCommitSpec')
    @KafkaClient
    static interface ProductClient {
        @Topic(ManualOffsetCommitSpec.TOPIC_SYNC)
        void send(Product product)
    }

    @Requires(property = 'spec.name', value = 'ManualOffsetCommitSpec')
    @Singleton
    static class ProductListener {

        List<Product> products = []

        @KafkaListener(offsetReset = EARLIEST, offsetStrategy = DISABLED)
        @Topic(ManualOffsetCommitSpec.TOPIC_SYNC)
        void receive(Product product,
                     long offset,
                     int partition,
                     String topic,
                     Consumer kafkaConsumer) {
            products.add(product)

            kafkaConsumer.commitSync(Collections.singletonMap(
                    new TopicPartition(topic, partition),
                    new OffsetAndMetadata(offset + 1, "my metadata")
            ))
        }
    }

    static class Product {
        String name
    }
}

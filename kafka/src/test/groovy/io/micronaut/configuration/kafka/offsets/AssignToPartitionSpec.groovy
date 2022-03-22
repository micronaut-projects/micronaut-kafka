package io.micronaut.configuration.kafka.offsets

import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.configuration.kafka.ConsumerAware
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

class AssignToPartitionSpec extends AbstractKafkaContainerSpec {

    public static final String TOPIC_SYNC = "AssignToPartitionSpec-products-sync"

    protected Map<String, Object> getConfiguration() {
        super.configuration + [(EMBEDDED_TOPICS): [TOPIC_SYNC]]
    }

    void "test manual offset commit"() {
        given:
        ProductClient client = context.getBean(ProductClient)
        ProductListener listener = context.getBean(ProductListener)

        when:
        client.send(new Product(name: "Apple"))
        client.send(new Product(name: "Orange"))
        client.send(new Product(name: "Banana"))

        then:
        conditions.eventually {
            listener.products.size() > 0
            listener.partitionsAssigned != null
            listener.partitionsAssigned.size() > 0
        }
    }

    @KafkaClient
    @Requires(property = 'spec.name', value = 'AssignToPartitionSpec')
    static interface ProductClient {
        @Topic(AssignToPartitionSpec.TOPIC_SYNC)
        void send(Product product)
    }

    @Singleton
    @Requires(property = 'spec.name', value = 'AssignToPartitionSpec')
    static class ProductListener implements ConsumerRebalanceListener, ConsumerAware {

        List<Product> products = []
        Consumer kafkaConsumer
        Collection<TopicPartition> partitionsAssigned

        @KafkaListener(offsetReset = EARLIEST)
        @Topic(AssignToPartitionSpec.TOPIC_SYNC)
        void receive(Product product) {
            products << product
        }

        @Override
        void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        }

        @Override
        void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            partitionsAssigned = partitions
            for (tp in partitions) {
                kafkaConsumer.seek(tp, 1)
            }
        }
    }

    static class Product {
        String name
    }
}

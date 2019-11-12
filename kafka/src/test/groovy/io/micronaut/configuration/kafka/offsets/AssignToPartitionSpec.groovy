/*
 * Copyright 2017-2019 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.kafka.offsets

import io.micronaut.configuration.kafka.ConsumerAware
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.CollectionUtils
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.testcontainers.containers.KafkaContainer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import javax.inject.Singleton

class AssignToPartitionSpec extends Specification {
    public static final String TOPIC_SYNC = "AssignToPartitionSpec-products-sync"
    @Shared @AutoCleanup KafkaContainer kafkaContainer = new KafkaContainer()
    @Shared @AutoCleanup ApplicationContext context
    def setupSpec() {
        kafkaContainer.start()
        context = ApplicationContext.run(
                CollectionUtils.mapOf(
                        "kafka.bootstrap.servers", kafkaContainer.getBootstrapServers(),
                        AbstractKafkaConfiguration.EMBEDDED_TOPICS, [TOPIC_SYNC]
                )
        )
    }
    void "test manual offset commit"() {
        given:
        ProductClient client = context.getBean(ProductClient)
        ProductListener listener = context.getBean(ProductListener)
        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

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
    static interface ProductClient {

        @Topic(ManualOffsetCommitSpec.TOPIC_SYNC)
        void send(Product product)
    }

    @Singleton
    static class ProductListener implements ConsumerRebalanceListener, ConsumerAware {

        List<Product> products = []
        Consumer kafkaConsumer

        Collection<TopicPartition> partitionsAssigned
        @KafkaListener(
                offsetReset = OffsetReset.EARLIEST
        )

        @Topic(ManualOffsetCommitSpec.TOPIC_SYNC)
        void receive(Product product) {
            products.add(product)
        }

        @Override
        void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        }

        @Override
        void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            partitionsAssigned = partitions
            for(tp in partitions) {
                kafkaConsumer.seek(tp, 1)
            }
        }

    }

    static class Product {
        String name
    }
}

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

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.CollectionUtils
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.TopicPartition
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import javax.inject.Singleton

class BatchManualOffsetCommitSpec extends Specification {
    public static final String TOPIC_SYNC = "BatchManualOffsetCommitSpec-products-sync"
    @Shared @AutoCleanup ApplicationContext context = ApplicationContext.run(
            CollectionUtils.mapOf(
                    "kafka.bootstrap.servers", 'localhost:${random.port}',
                    AbstractKafkaConfiguration.EMBEDDED, true,
                    AbstractKafkaConfiguration.EMBEDDED_TOPICS, [TOPIC_SYNC]
            )
    )
    void "test manual offset commit"() {
        given:
        ProductClient client = context.getBean(ProductClient)
        ProductListener listener = context.getBean(ProductListener)
        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

        when:
        client.send(new Product(name: "Apple"))
        client.send(new Product(name: "Orange"))

        then:
        conditions.eventually {
            listener.products.size() == 2
            listener.products.find() { it.name == "Apple"}
        }
    }

    @KafkaClient
    static interface ProductClient {

        @Topic(ManualOffsetCommitSpec.TOPIC_SYNC)
        void send(Product product)
    }

    @Singleton
    static class ProductListener {

        List<Product> products = []

        @KafkaListener(
                offsetReset = OffsetReset.EARLIEST,
                offsetStrategy = OffsetStrategy.DISABLED,
                batch = true
        )
        @Topic(ManualOffsetCommitSpec.TOPIC_SYNC)
        void receive(
                List<Product> products,
                List<Long> offsets,
                List<Integer> partitions,
                List<String> topics,
                Consumer kafkaConsumer) {
            int i = 0
            for(p in products) {

                this.products.add(p)

                String topic = topics.get(i)
                int partition = partitions.get(i)
                long offset = offsets.get(i)

                kafkaConsumer.commitSync(Collections.singletonMap(
                        new TopicPartition(topic, partition),
                        new org.apache.kafka.clients.consumer.OffsetAndMetadata(offset + 1, "my metadata")
                ))
            }
        }
    }

    static class Product {
        String name
    }
}

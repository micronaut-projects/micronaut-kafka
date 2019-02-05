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
package io.micronaut.configuration.kafka.docs.consumer.offsets.rebalance;

import io.micronaut.configuration.kafka.docs.consumer.config.Product;
// tag::imports[]
import io.micronaut.configuration.kafka.KafkaConsumerAware;
import io.micronaut.configuration.kafka.annotation.*;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nonnull;
import java.util.Collection;
// end::imports[]


// tag::clazz[]
@KafkaListener
public class ProductListener implements ConsumerRebalanceListener, KafkaConsumerAware {

    private KafkaConsumer consumer;

    @Override
    public void setKafkaConsumer(@Nonnull KafkaConsumer consumer) { // <1>
        this.consumer = consumer;
    }

    @Topic("awesome-products")
    void receive(Product product) {
        // process product
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) { // <2>
        // save offsets here
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) { // <3>
        // seek to offset here
        for (TopicPartition partition : partitions) {
            consumer.seek(partition, 1);
        }
    }
}
// end::clazz[]
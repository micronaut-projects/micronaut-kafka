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
package io.micronaut.configuration.kafka.docs.consumer.config;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.context.annotation.Property;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
// end::imports[]

// tag::clazz[]
@KafkaListener(
    groupId = "products",
    pollTimeout = "500ms",
    properties = @Property(name = ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, value = "10000")
)
public class ProductListener {
// end::clazz[]

    // tag::method[]
    @Topic("awesome-products")
    public void receive(
            @KafkaKey String brand, // <1>
            Product product, // <2>
            long offset, // <3>
            int partition, // <4>
            String topic, // <5>
            long timestamp) { // <6>
        System.out.println("Got Product - " + product.getName() + " by " + brand);
    }
    // end::method[]

    // tag::consumeRecord[]
    @Topic("awesome-products")
    public void receive(ConsumerRecord<String, Product> record) { // <1>
        Product product = record.value(); // <2>
        String brand = record.key(); // <3>
        System.out.println("Got Product - " + product.getName() + " by " + brand);
    }
    // end::consumeRecord[]
}
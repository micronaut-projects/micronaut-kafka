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
package io.micronaut.configuration.kafka.docs.consumer.sendto;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.configuration.kafka.docs.consumer.config.Product;
import io.micronaut.messaging.annotation.SendTo;
import io.reactivex.Single;
import io.reactivex.functions.Function;
// end::imports[]

@KafkaListener
public class ProductListener {

    // tag::method[]
    @Topic("awesome-products") // <1>
    @SendTo("product-quantities") // <2>
    public int receive(
            @KafkaKey String brand,
            Product product) {
        System.out.println("Got Product - " + product.getName() + " by " + brand);

        return product.getQuantity(); // <3>
    }
    // end::method[]

    // tag::reactive[]
    @Topic("awesome-products") // <1>
    @SendTo("product-quantities") // <2>
    public Single<Integer> receiveProduct(
            @KafkaKey String brand,
            Single<Product> productSingle) {

        return productSingle.map(product -> {
            System.out.println("Got Product - " + product.getName() + " by " + brand);
            return product.getQuantity(); // <3>
        });
    }
    // end::reactive[]
}

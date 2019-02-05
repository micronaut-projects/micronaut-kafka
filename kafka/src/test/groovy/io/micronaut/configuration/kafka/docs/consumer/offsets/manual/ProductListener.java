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
package io.micronaut.configuration.kafka.docs.consumer.offsets.manual;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.configuration.kafka.docs.consumer.config.Product;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import java.util.Collections;
// end::imports[]

class ProductListener {


    // tag::method[]
    @KafkaListener(
            offsetReset = OffsetReset.EARLIEST,
            offsetStrategy = OffsetStrategy.DISABLED // <1>
    )
    @Topic("awesome-products")
    void receive(
            Product product,
            long offset,
            int partition,
            String topic,
            KafkaConsumer kafkaConsumer) { // <2>
        // process product record

        // commit offsets
        kafkaConsumer.commitSync(Collections.singletonMap( // <3>
                new TopicPartition(topic, partition),
                new OffsetAndMetadata(offset + 1, "my metadata")
        ));

    }
    // end::method[]
}

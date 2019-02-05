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
package io.micronaut.configuration.kafka.docs.producer.headers;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.messaging.annotation.Header;
// end::imports[]

// tag::clazz[]
@KafkaClient(id="product-client")
@Header(name = "X-Token", value = "${my.application.token}")
public interface ProductClient {
// end::clazz[]
}


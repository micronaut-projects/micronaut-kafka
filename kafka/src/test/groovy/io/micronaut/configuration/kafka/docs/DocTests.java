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
package io.micronaut.configuration.kafka.docs;

import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration;
import io.micronaut.configuration.kafka.docs.quickstart.ProductClient;
import io.micronaut.context.ApplicationContext;
import io.micronaut.core.util.CollectionUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DocTests {

    static ApplicationContext applicationContext;

    @BeforeClass
    public static void setup() {
        applicationContext = ApplicationContext.run(
                CollectionUtils.mapOf(
                        "kafka.bootstrap.servers", "localhost:${random.port}",
                        AbstractKafkaConfiguration.EMBEDDED, true
            )
        );
    }

    @AfterClass
    public static void cleanup() {
        applicationContext.stop();
    }


    @Test
    public void testSendProduct() {
        // tag::quickstart[]
        ProductClient client = applicationContext.getBean(ProductClient.class);
        client.sendProduct("Nike", "Blue Trainers");
        // end::quickstart[]
    }
}

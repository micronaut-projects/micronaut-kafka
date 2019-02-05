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
package io.micronaut.configuration.kafka.docs.producer.inject;

import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration;
import io.micronaut.configuration.kafka.docs.consumer.batch.Book;
import io.micronaut.context.ApplicationContext;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class BookSenderTest {

    // tag::test[]
    @Test
    public void testBookSender() throws IOException {
        Map<String, Object> config = Collections.singletonMap( // <1>
                AbstractKafkaConfiguration.EMBEDDED, true
        );

        try (ApplicationContext ctx = ApplicationContext.run(config)) {
            BookSender bookSender = ctx.getBean(BookSender.class); // <2>
            Book book = new Book();
            book.setTitle("The Stand");
            bookSender.send("Stephen King", book);
        }
    }
    // end::test[]
}

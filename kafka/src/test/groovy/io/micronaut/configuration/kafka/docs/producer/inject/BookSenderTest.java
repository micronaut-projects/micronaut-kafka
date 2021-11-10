package io.micronaut.configuration.kafka.docs.producer.inject;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import io.micronaut.configuration.kafka.docs.consumer.batch.Book;
import io.micronaut.context.ApplicationContext;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.Map;

public class BookSenderTest {

    // tag::test[]
    @Test
    public void testBookSender() {
        try (KafkaContainer container = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))) {
            container.start();
            Map<String, Object> config = Collections.singletonMap( // <1>
               ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
               container.getBootstrapServers()
            );

            try (ApplicationContext ctx = ApplicationContext.run(config)) {
                BookSender bookSender = ctx.getBean(BookSender.class); // <2>
                Book book = new Book();
                book.setTitle("The Stand");
                bookSender.send("Stephen King", book);
            }
        }

    }
    // end::test[]
}

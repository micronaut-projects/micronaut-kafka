package io.micronaut.configuration.kafka.embedded;

import io.micronaut.context.ApplicationContext;

import java.util.Collections;

import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED;
import static io.micronaut.context.env.Environment.TEST;

public class EmbeddedMain {

    public static void main(String...args) {
        ApplicationContext applicationContext = ApplicationContext.run(
                Collections.singletonMap(EMBEDDED, true),
                TEST
        );
        KafkaEmbedded embedded = applicationContext.getBean(KafkaEmbedded.class);
    }
}

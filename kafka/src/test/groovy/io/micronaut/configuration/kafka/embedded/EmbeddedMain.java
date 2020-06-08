
package io.micronaut.configuration.kafka.embedded;

import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.Environment;

import java.util.Collections;

public class EmbeddedMain {

    public static void main(String...args) {
        ApplicationContext applicationContext = ApplicationContext.run(
                Collections.singletonMap(
                        AbstractKafkaConfiguration.EMBEDDED, true
                )
        , Environment.TEST);
        KafkaEmbedded embedded = applicationContext.getBean(KafkaEmbedded.class);
    }
}

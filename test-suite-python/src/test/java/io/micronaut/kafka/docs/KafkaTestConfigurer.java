package io.micronaut.kafka.docs;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.ApplicationContextConfigurer;
import io.micronaut.context.annotation.ContextConfigurer;
import io.micronaut.context.env.Environment;
import io.micronaut.context.env.PropertySource;
import io.micronaut.testcontainers.kafka.Kafka;

import java.util.Map;

/**
 * Supplies the {@code kafka.bootstrap.servers} of the shared Kafka test container to the Python tests
 * run with the {@code kafka} environment, like {@code TestPropertyProvider} does for the Java, Kotlin
 * and Groovy suites.
 * <p>
 * The configurer is written in Java because Micronaut Test calls {@code TestPropertyProvider} before the
 * application context, and with it the GraalPy runtime, exists, so a Python test class cannot supply
 * the container properties. It uses the {@link #configure(ApplicationContext)} callback because the
 * {@link io.micronaut.context.ApplicationContextBuilder} is configured before {@code @MicronautTest}
 * selects the environments, so the {@code kafka} environment can only be checked on the built context.
 */
@ContextConfigurer
public class KafkaTestConfigurer implements ApplicationContextConfigurer {

    private static final String ENVIRONMENT = "kafka";

    @Override
    public void configure(ApplicationContext applicationContext) {
        Environment environment = applicationContext.getEnvironment();
        if (environment.getActiveNames().contains(ENVIRONMENT)) {
            environment.addPropertySource(PropertySource.of(ENVIRONMENT, Map.copyOf(Kafka.getProperties())));
        }
    }
}

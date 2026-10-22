package io.micronaut.configuration.kafka

import io.micronaut.configuration.kafka.config.DefaultKafkaConsumerConfiguration
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.jspecify.annotations.Nullable
import spock.lang.Specification

class ConsumerFactorySpec extends Specification {

    void "consumers are created by the default consumer factory"() {
        given:
        ApplicationContext context = ApplicationContext.run(['kafka.bootstrap.servers': 'localhost:9092', 'kafka.key.deserializer': StringDeserializer.name, 'kafka.value.deserializer': StringDeserializer.name])

        when:
        Consumer<?, ?> consumer = context.createBean(Consumer, context.createBean(DefaultKafkaConsumerConfiguration))

        then:
        consumer instanceof KafkaConsumer

        cleanup:
        consumer?.close()
        context.close()
    }

    void "a replacement consumer factory creates the consumers"() {
        given:
        ApplicationContext context = ApplicationContext.run([
                'spec.name'              : 'ConsumerFactorySpec',
                'kafka.bootstrap.servers': 'localhost:9092'
        ])

        when:
        Consumer<?, ?> consumer = context.createBean(Consumer, context.createBean(DefaultKafkaConsumerConfiguration))

        then:
        consumer instanceof MockConsumer
        context.getBean(MockConsumerFactory).configs.any { it.getProperty('bootstrap.servers') == 'localhost:9092' }

        cleanup:
        context.close()
    }

    void "the admin client is available as the Admin interface"() {
        given:
        ApplicationContext context = ApplicationContext.run(['kafka.bootstrap.servers': 'localhost:9092'])

        expect:
        context.getBean(Admin) != null

        cleanup:
        context.close()
    }

    @Singleton
    @Replaces(DefaultConsumerFactory)
    @Requires(property = 'spec.name', value = 'ConsumerFactorySpec')
    static class MockConsumerFactory implements ConsumerFactory {

        final List<Properties> configs = []

        @Override
        <K, V> Consumer<K, V> createConsumer(Properties config, @Nullable Deserializer<K> kd, @Nullable Deserializer<V> vd) {
            configs << config
            return new MockConsumer<K, V>("earliest")
        }
    }
}

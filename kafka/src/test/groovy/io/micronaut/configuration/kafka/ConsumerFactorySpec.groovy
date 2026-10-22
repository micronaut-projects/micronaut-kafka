package io.micronaut.configuration.kafka

import io.micronaut.configuration.kafka.config.DefaultKafkaConsumerConfiguration
import io.micronaut.configuration.kafka.admin.KafkaNewTopics
import io.micronaut.configuration.kafka.processor.KafkaConsumerGroupManager
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.CreateTopicsOptions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.jspecify.annotations.Nullable
import spock.lang.Specification

import java.time.Duration

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
        consumer?.close()
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

    void "an Admin bean that is not an AdminClient creates topics and manages consumer groups"() {
        given:
        ApplicationContext context = ApplicationContext.run([
                'spec.name'              : 'ConsumerFactorySpec.Admin',
                'kafka.bootstrap.servers': 'localhost:9092',
                'kafka.admin.enabled'    : false
        ])

        expect: "the default AdminClient is disabled, so the custom Admin is the only one"
        !context.containsBean(AdminClient)
        !(context.getBean(Admin) instanceof AdminClient)

        and: "the beans that need an Admin are created with it"
        context.getBean(KafkaNewTopics) != null
        context.getBean(CustomAdminFactory).createdTopics*.name() == ['custom-admin-topic']
        context.getBean(KafkaConsumerGroupManager) != null

        cleanup:
        context.close()
    }

    @Factory
    @Requires(property = 'spec.name', value = 'ConsumerFactorySpec.Admin')
    static class CustomAdminFactory {

        final List<NewTopic> createdTopics = []

        @Singleton
        NewTopic newTopic() {
            return new NewTopic('custom-admin-topic', 1, (short) 1)
        }

        @Singleton
        Admin admin() {
            // an Admin that is not an AdminClient, like the OKafka admin client
            return [
                    createTopics: { Collection<NewTopic> topics, CreateTopicsOptions options ->
                        createdTopics.addAll(topics)
                        return null
                    },
                    close       : { Duration timeout -> }
            ] as Admin
        }
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

package io.micronaut.configuration.kafka

import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration
import io.micronaut.configuration.kafka.config.AbstractKafkaConsumerConfiguration
import io.micronaut.configuration.kafka.config.AbstractKafkaProducerConfiguration
import io.micronaut.configuration.kafka.config.KafkaConsumerConfiguration
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.EnvironmentPropertySource
import io.micronaut.context.env.MapPropertySource
import io.micronaut.context.exceptions.NoSuchBeanException
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import spock.lang.AutoCleanup
import spock.lang.Issue
import spock.lang.Specification

import java.nio.charset.StandardCharsets

import static io.micronaut.context.env.PropertySource.PropertyConvention.ENVIRONMENT_VARIABLE
import static org.apache.kafka.clients.consumer.ConsumerConfig.*
import static org.apache.kafka.clients.producer.ProducerConfig.*

class KafkaConfigurationSpec extends Specification {

    @AutoCleanup ApplicationContext applicationContext

    void "test default consumer configuration"() {
        given:
        applicationContext = ApplicationContext.builder().enableDefaultPropertySources(false)
                .properties(("kafka." + KEY_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name,
                            ("kafka." + VALUE_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name)
                .run(ApplicationContext.class);

        when:
        AbstractKafkaConsumerConfiguration config = applicationContext.getBean(AbstractKafkaConsumerConfiguration)
        Properties props = config.getConfig()

        then:
        props[BOOTSTRAP_SERVERS_CONFIG] == AbstractKafkaConfiguration.DEFAULT_BOOTSTRAP_SERVERS

        when:
        Consumer consumer = applicationContext.createBean(Consumer, config)

        then:
        consumer != null

        cleanup:
        consumer.close()
    }

    void "test custom consumer deserializer"() {
        given: "config with specific deserializer encodings"
        applicationContext = ApplicationContext.run(
                ("kafka." + KEY_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name,
                ("kafka." + KEY_DESERIALIZER_CLASS_CONFIG + ".encoding"): StandardCharsets.US_ASCII.name(),
                ("kafka." + VALUE_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name,
                ("kafka." + VALUE_DESERIALIZER_CLASS_CONFIG + ".encoding"): StandardCharsets.ISO_8859_1.name(),
        )

        when: "custom deserializers are set"
        AbstractKafkaConsumerConfiguration config = applicationContext.getBean(AbstractKafkaConsumerConfiguration)
        config.setKeyDeserializer(new StringDeserializer())
        config.setValueDeserializer(new StringDeserializer())

        and: "a consumer is created"
        KafkaConsumer consumer = applicationContext.createBean(Consumer, config)

        then: "the new consumer's deserializers have the configured encoding"
        consumer != null
        (consumer.keyDeserializer as StringDeserializer).encoding == StandardCharsets.US_ASCII.name()
        (consumer.valueDeserializer as StringDeserializer).encoding == StandardCharsets.ISO_8859_1.name()

        cleanup:
        consumer.close()
    }

    void "test custom producer serializer"() {
        given: "config with specific serializer encodings"
        applicationContext = ApplicationContext.run(
                ("kafka." + KEY_SERIALIZER_CLASS_CONFIG): StringSerializer.name,
                ("kafka." + KEY_SERIALIZER_CLASS_CONFIG + ".encoding"): StandardCharsets.US_ASCII.name(),
                ("kafka." + VALUE_SERIALIZER_CLASS_CONFIG): StringSerializer.name,
                ("kafka." + VALUE_SERIALIZER_CLASS_CONFIG + ".encoding"): StandardCharsets.ISO_8859_1.name(),
        )

        when: "custom serializers are set"
        AbstractKafkaProducerConfiguration config = applicationContext.getBean(AbstractKafkaProducerConfiguration)
        config.setKeySerializer(new StringSerializer())
        config.setValueSerializer(new StringSerializer())

        and: "a producer is created"
        KafkaProducer producer = applicationContext.createBean(Producer, config)

        then: "the new producer's serializers have the configured encoding"
        producer != null
        (producer.keySerializer as StringSerializer).encoding == StandardCharsets.US_ASCII.name()
        (producer.valueSerializer as StringSerializer).encoding == StandardCharsets.ISO_8859_1.name()

        cleanup:
        producer.close()
    }

    void "test configure default properties"() {
        given:
        applicationContext = ApplicationContext.run(
                ('kafka.' + BOOTSTRAP_SERVERS_CONFIG): "localhost:1111",
                ('kafka.' + GROUP_ID_CONFIG): "mygroup",
                ('kafka.' + MAX_POLL_RECORDS_CONFIG): "100",
                ("kafka." + KEY_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name,
                ("kafka." + VALUE_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name
        )

        when:
        AbstractKafkaConsumerConfiguration config = applicationContext.getBean(AbstractKafkaConsumerConfiguration)
        Properties props = config.getConfig()

        then:
        props[BOOTSTRAP_SERVERS_CONFIG] == "localhost:1111"
        props[GROUP_ID_CONFIG] == "mygroup"
        props[MAX_POLL_RECORDS_CONFIG] == "100"

        when:
        Consumer consumer = applicationContext.createBean(Consumer, config)

        then:
        consumer != null

        cleanup:
        consumer.close()
    }

    void "test override consumer default properties"() {
        given:
        applicationContext = ApplicationContext.run(
                ('kafka.' + BOOTSTRAP_SERVERS_CONFIG): "localhost:1111",
                ('kafka.' + GROUP_ID_CONFIG): "mygroup",
                ('kafka.consumers.default.' + BOOTSTRAP_SERVERS_CONFIG): "localhost:2222",
                ('kafka.' + GROUP_ID_CONFIG): "mygroup",
                ('kafka.consumers.default.' + MAX_POLL_RECORDS_CONFIG): "100",
                ("kafka.consumers.default." + KEY_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name,
                ("kafka.consumers.default." + VALUE_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name
        )

        when:
        KafkaConsumerConfiguration config = applicationContext.getBean(KafkaConsumerConfiguration)
        Properties props = config.getConfig()

        then:
        props[BOOTSTRAP_SERVERS_CONFIG] == "localhost:2222"
        props[GROUP_ID_CONFIG] == "mygroup"
        props[MAX_POLL_RECORDS_CONFIG] == "100"

        when:
        Consumer consumer = applicationContext.createBean(Consumer, config)

        then:
        consumer != null

        cleanup:
        consumer.close()
    }

    void "test configure list fields default properties"() {
        given:
        applicationContext = ApplicationContext.run(
                ('kafka.' + BOOTSTRAP_SERVERS_CONFIG): ["localhost:1111", "localhost:1112"],
                ('kafka.' + GROUP_ID_CONFIG): "mygroup",
                ("kafka.consumers.default." + KEY_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name,
                ("kafka.consumers.default." + VALUE_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name
        )

        when:
        AbstractKafkaConsumerConfiguration config = applicationContext.getBean(AbstractKafkaConsumerConfiguration)
        Properties props = config.getConfig()

        then:
        props[BOOTSTRAP_SERVERS_CONFIG] == "localhost:1111,localhost:1112"
        props[GROUP_ID_CONFIG] == "mygroup"

        when:
        Consumer consumer = applicationContext.createBean(Consumer, config)

        then:
        consumer != null

        cleanup:
        consumer.close()
    }

    @Issue('https://github.com/micronaut-projects/micronaut-kafka/issues/286')
    void 'test environment property overrides'() {
        given:
        def yamlPropertySource = new FakeYamlPropertySource()
        def environmentPropertySource = new FakeEnvPropertySource()

        Map yamlListConfig = ['kafka.bootstrap.servers[0]': 'localhost:1111',
                              'kafka.bootstrap.servers': ['localhost:1111', 'localhost:2222'],
                              'kafka.bootstrap-servers': ['localhost:1111', 'localhost:2222'],
                              'kafka.bootstrap.servers[1]': 'localhost:2222']

        Map yamlSingleConfig = ['kafka.bootstrap.servers': 'localhost:1111',
                                'kafka.bootstrap-servers': 'localhost:1111']

        Map envListConfig = [KAFKA_BOOTSTRAP_SERVERS: 'localhost:3333,localhost:4444']

        Map envSingleConfig = [KAFKA_BOOTSTRAP_SERVERS: 'localhost:3333']

        yamlPropertySource.map.putAll yamlListConfig
        environmentPropertySource.map.putAll envListConfig

        when: 'only yaml source, list, expect 1111,2222'
        applicationContext = ApplicationContext.builder()
                .propertySources(yamlPropertySource)
                .start()
        def config = applicationContext.getBean(AbstractKafkaConsumerConfiguration)
        Properties props = config.config

        then:
        props[BOOTSTRAP_SERVERS_CONFIG] == 'localhost:1111,localhost:2222'

        when: 'only env source, list, expect 3333,4444'
        applicationContext.close()
        applicationContext = ApplicationContext.builder()
                .propertySources(environmentPropertySource)
                .start()
        config = applicationContext.getBean(AbstractKafkaConsumerConfiguration)
        props = config.config

        then:
        props[BOOTSTRAP_SERVERS_CONFIG] == 'localhost:3333,localhost:4444'

        when: 'both sources, both list, expect 3333,4444'
        applicationContext.close()
        applicationContext = ApplicationContext.builder()
                .propertySources(yamlPropertySource, environmentPropertySource)
                .start()
        config = applicationContext.getBean(AbstractKafkaConsumerConfiguration)
        props = config.config

        then:
        props[BOOTSTRAP_SERVERS_CONFIG] == 'localhost:3333,localhost:4444'

        when: 'both sources, single yaml, list env, expect 3333,4444'

        yamlPropertySource.map.putAll yamlSingleConfig
        environmentPropertySource.map.putAll envListConfig

        applicationContext.close()
        applicationContext = ApplicationContext.builder()
                .propertySources(yamlPropertySource, environmentPropertySource)
                .start()
        config = applicationContext.getBean(AbstractKafkaConsumerConfiguration)
        props = config.config

        then:
        props[BOOTSTRAP_SERVERS_CONFIG] == 'localhost:3333,localhost:4444'

        when: 'both sources, list yaml, single env, expect 3333'

        yamlPropertySource.map.putAll yamlListConfig
        environmentPropertySource.map.putAll envSingleConfig

        applicationContext.close()
        applicationContext = ApplicationContext.builder()
                .propertySources(yamlPropertySource, environmentPropertySource)
                .start()
        config = applicationContext.getBean(AbstractKafkaConsumerConfiguration)
        props = config.config

        then:
        props[BOOTSTRAP_SERVERS_CONFIG] == 'localhost:3333'

        when: 'both sources, both single, expect 3333'

        yamlPropertySource.map.putAll yamlSingleConfig
        environmentPropertySource.map.putAll envSingleConfig

        applicationContext.close()
        applicationContext = ApplicationContext.builder()
                .propertySources(yamlPropertySource, environmentPropertySource)
                .start()
        config = applicationContext.getBean(AbstractKafkaConsumerConfiguration)
        props = config.config

        then:
        props[BOOTSTRAP_SERVERS_CONFIG] == 'localhost:3333'
    }

    void "test disabled"() {
        given:
        applicationContext = ApplicationContext.run(["kafka.enabled": false])

        when:
        applicationContext.getBean(AbstractKafkaConfiguration)

        then:
        thrown(NoSuchBeanException)

        when:
        applicationContext.getBean(ConsumerRegistry)

        then:
        thrown(NoSuchBeanException)

        when:
        applicationContext.getBean(AbstractKafkaConsumerConfiguration)

        then:
        thrown(NoSuchBeanException)

        when:
        applicationContext.getBean(AbstractKafkaProducerConfiguration)

        then:
        thrown(NoSuchBeanException)
    }

}

class FakeYamlPropertySource extends MapPropertySource {

    static final Map map = [:]

    FakeYamlPropertySource() {
        super('fake yml', map)
    }

    final int order = EnvironmentPropertySource.POSITION - 1
}

class FakeEnvPropertySource extends MapPropertySource {

    static final Map map = [:]

    FakeEnvPropertySource() {
        super('fake env', map)
    }

    final int order = EnvironmentPropertySource.POSITION

    final PropertyConvention convention = ENVIRONMENT_VARIABLE
}

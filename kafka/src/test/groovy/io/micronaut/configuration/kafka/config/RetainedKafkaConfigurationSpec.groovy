package io.micronaut.configuration.kafka.config

import io.micronaut.configuration.kafka.admin.AdminClientFactory
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.MapPropertySource
import io.micronaut.context.env.PropertySource
import jakarta.inject.Singleton
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClient
import spock.lang.AutoCleanup
import spock.lang.Specification

import java.lang.reflect.InvocationHandler
import java.lang.reflect.Proxy

class RetainedKafkaConfigurationSpec extends Specification {

    @AutoCleanup
    ApplicationContext context

    private static final Map<String, Object> PROPERTIES = [
        'kafka.bootstrap.servers'         : ['localhost:9092', 'localhost:9093'],
        'kafka.health.enabled'            : false,
        'kafka.ssl.endpoint.identification.algorithm': '${kafka.algorithm-name}',
        'kafka.algorithm-name'            : 'https',
        'kafka.request.timeout.ms'        : 20000,
        'kafka.someCamelCase.key'         : 'camel',
        'kafka.indexed[0]'                : 'zero',
        'kafka.indexed[1]'                : 'one',
        'kafka.nested'                    : [deep: [value: 'nested']],
        'kafka.producers.default.acks'    : 'left-out',
        'kafka.streams.default.num'       : 'left-out',
        'kafka.embedded.enabled'          : false,
    ]

    void "in development mode the retained configuration holds what the default configuration holds"() {
        given:
        context = ApplicationContext.builder()
            .properties(PROPERTIES + ['micronaut.dev.enabled': true])
            .propertySources(new MapPropertySource('env', [KAFKA_SASL_MECHANISM: 'PLAIN']) {
                @Override
                PropertySource.PropertyConvention getConvention() {
                    PropertySource.PropertyConvention.ENVIRONMENT_VARIABLE
                }
            })
            .start()

        when:
        Properties retained = context.getBean(RetainedKafkaConfiguration).config
        Properties production = context.getBean(KafkaDefaultConfiguration).config

        then: 'the keys the environment resolves, the environment variables among them, and their values'
        retained == production
        retained['bootstrap.servers'] == 'localhost:9092,localhost:9093'
        retained['sasl.mechanism'] == 'PLAIN'
        retained['ssl.endpoint.identification.algorithm'] == 'https'
        retained['security.protocol'] == 'SSL'
        !retained.containsKey('producers.default.acks')
    }

    void "in development mode the bootstrap servers default as they do in the default configuration"() {
        given:
        context = ApplicationContext.run(['kafka.health.enabled': false, 'micronaut.dev.enabled': true])

        expect:
        context.getBean(RetainedKafkaConfiguration).config == context.getBean(KafkaDefaultConfiguration).config
        context.getBean(RetainedKafkaConfiguration).config['bootstrap.servers'] == AbstractKafkaConfiguration.DEFAULT_BOOTSTRAP_SERVERS
    }

    void "the retained configuration is only exposed under its own type, and the admin client is built from it"() {
        given:
        context = ApplicationContext.run(['kafka.health.enabled': false, 'micronaut.dev.enabled': true])

        expect:
        context.getBeansOfType(AbstractKafkaConfiguration).every { it !instanceof RetainedKafkaConfiguration }
        context.getBeanDefinition(AdminClient).requiredComponents.contains(RetainedKafkaConfiguration)
        !context.getBeanDefinition(AdminClient).requiredComponents.contains(KafkaDefaultConfiguration)
    }

    void "outside development mode there is no retained configuration, and the admin client is built from the default one"() {
        given:
        context = ApplicationContext.run(['kafka.health.enabled': false, 'kafka.bootstrap.servers': 'localhost:9092'])

        expect:
        !context.containsBean(RetainedKafkaConfiguration)
        context.getBeanDefinition(AdminClient).requiredComponents.contains(KafkaDefaultConfiguration)
    }

    void "an admin client that replaces the one of the factory replaces it with development mode on or off (dev: #development)"() {
        given:
        context = ApplicationContext.run(['kafka.health.enabled': false, 'kafka.bootstrap.servers': 'localhost:9092',
                                          'spec.name': 'RetainedKafkaConfigurationSpec', 'micronaut.dev.enabled': development])

        expect:
        context.getBean(Admin).is(context.getBean(ReplacingAdminFactory).admin)

        where:
        development << [true, false]
    }

    @Factory
    @Requires(property = 'spec.name', value = 'RetainedKafkaConfigurationSpec')
    static class ReplacingAdminFactory {

        final Admin admin = Proxy.newProxyInstance(Admin.classLoader, [Admin] as Class[], { proxy, method, args -> null } as InvocationHandler) as Admin

        @Singleton
        @Replaces(bean = AdminClient, factory = AdminClientFactory)
        Admin replacingAdmin() {
            admin
        }
    }
}

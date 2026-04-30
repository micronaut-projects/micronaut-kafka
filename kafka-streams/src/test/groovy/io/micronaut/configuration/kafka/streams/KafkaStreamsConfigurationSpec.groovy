package io.micronaut.configuration.kafka.streams

import io.micronaut.configuration.kafka.config.KafkaDefaultConfiguration
import io.micronaut.context.env.Environment
import io.micronaut.runtime.ApplicationConfiguration
import org.apache.kafka.streams.StreamsConfig
import spock.lang.Specification

class KafkaStreamsConfigurationSpec extends Specification {

    void "test environment uses an application-specific kafka streams state directory"() {
        given:
        def environment = Stub(Environment) {
            getActiveNames() >> ([Environment.TEST] as Set)
            containsProperties('kafka') >> false
        }
        def applicationConfiguration = Stub(ApplicationConfiguration) {
            getName() >> Optional.of('state-dir-spec')
        }
        def defaultConfiguration = new KafkaDefaultConfiguration(environment)

        when:
        def configuration = new DefaultKafkaStreamsConfiguration(defaultConfiguration, applicationConfiguration, environment)

        then:
        configuration.config.getProperty(StreamsConfig.STATE_DIR_CONFIG) ==
            new File(System.getProperty('java.io.tmpdir'), 'state-dir-spec').absolutePath
    }
}

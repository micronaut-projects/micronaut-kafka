package io.micronaut.configuration.kafka.streams.reload

import io.micronaut.context.ApplicationContext
import io.micronaut.context.reload.ClassChangeEvent
import io.micronaut.context.reload.ReloadStrategy
import spock.lang.Specification

/**
 * The streams reloader exists in development mode only. The rebuild of running streams, which needs a broker, is
 * covered by the KafkaStreamsReloadTest of test-suite-dev-reload.
 */
class KafkaStreamsReloaderSpec extends Specification {

    private static final String RELOADER = 'io.micronaut.configuration.kafka.streams.DevelopmentKafkaStreamsReloader'

    void "in development mode the reloader exists, and a class change with no streams running builds nothing"() {
        given:
        ApplicationContext context = ApplicationContext.run(properties() + ['micronaut.dev.enabled': true])

        expect:
        context.containsBean(Class.forName(RELOADER))

        when:
        context.publishEvent(new ClassChangeEvent(KafkaStreamsReloaderSpec, 1, [KafkaStreamsReloaderSpec.classLoader] as Set, KafkaStreamsReloaderSpec.classLoader, [], ReloadStrategy.RELOAD))
        context.publishEvent(new ClassChangeEvent(KafkaStreamsReloaderSpec, 1, [] as Set, KafkaStreamsReloaderSpec.classLoader, [], ReloadStrategy.RESTART))

        then:
        noExceptionThrown()

        cleanup:
        context.close()
    }

    void "outside development mode there is no reloader"() {
        given:
        ApplicationContext context = ApplicationContext.run(properties())

        expect:
        !context.containsBean(Class.forName(RELOADER))

        cleanup:
        context.close()
    }

    private static Map<String, Object> properties() {
        return ['kafka.bootstrap.servers': 'localhost:9', 'kafka.health.enabled': false, 'kafka.streams.enabled': false, 'kafka.test.initializer.enabled': false]
    }
}

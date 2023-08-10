package io.micronaut.configuration.kafka.health

import io.micronaut.configuration.kafka.AbstractKafkaSpec
import io.micronaut.context.ApplicationContext
import io.micronaut.core.io.socket.SocketUtils
import io.micronaut.management.health.indicator.HealthResult

import static io.micronaut.health.HealthStatus.DOWN
import static io.micronaut.health.HealthStatus.UP

class RestrictedKafkaHealthIndicatorSpec extends AbstractKafkaSpec {

    void "test restricted kafka health indicator - UP"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(configuration +
                ['kafka.health.restricted': 'true'])

        when:
        RestrictedKafkaHealthIndicator healthIndicator = applicationContext.getBean(RestrictedKafkaHealthIndicator)
        HealthResult result = healthIndicator.result.next().block()

        then:
        result.status == UP
        result.details['clusterId'] != null

        cleanup:
        applicationContext.close()
    }

    void "test restricted kafka health indicator - DOWN"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(configuration +
                ['kafka.health.restricted': 'true', 'kafka.bootstrap.servers': 'localhost:' + SocketUtils.findAvailableTcpPort()]
        )

        when:
        RestrictedKafkaHealthIndicator healthIndicator = applicationContext.getBean(RestrictedKafkaHealthIndicator)
        HealthResult result = healthIndicator.result.next().block()

        then:
        result.status == DOWN
        result.details['error'] != null

        cleanup:
        applicationContext.close()
    }

    void "test restricted kafka health indicator is disabled"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(configuration +
                ['kafka.health.enabled': enabled, 'kafka.health.restricted': restricted]
        )

        when:
        Optional<RestrictedKafkaHealthIndicator> optional = applicationContext.findBean(RestrictedKafkaHealthIndicator)

        then:
        optional.isEmpty()

        cleanup:
        applicationContext.close()

        where:
        enabled | restricted
        false   | false
        false   | 'true'
        true    | 'false'
        true    | 'no'
        true    | ''
    }
}

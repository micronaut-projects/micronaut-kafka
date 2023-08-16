package io.micronaut.configuration.kafka.health

import io.micronaut.configuration.kafka.AbstractKafkaSpec
import io.micronaut.context.ApplicationContext
import io.micronaut.core.io.socket.SocketUtils
import io.micronaut.core.util.StringUtils
import io.micronaut.management.health.indicator.HealthResult

import static io.micronaut.health.HealthStatus.DOWN
import static io.micronaut.health.HealthStatus.UP

class RestrictedKafkaHealthIndicatorSpec extends AbstractKafkaSpec {

    void "test restricted kafka health indicator - UP"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(configuration +
                ['kafka.health.restricted': StringUtils.TRUE, 'endpoints.health.details-visible': StringUtils.TRUE])
        KafkaHealthIndicator healthIndicator = applicationContext.getBean(KafkaHealthIndicator)

        expect:
        conditions.eventually {
            HealthResult result = healthIndicator.result.next().block()
            result.status == UP && result.details && result.details['clusterId'] != null
        }

        cleanup:
        applicationContext.close()
    }

    void "test restricted kafka health indicator - DOWN"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(configuration +
                ['kafka.health.restricted': StringUtils.TRUE, 'kafka.bootstrap.servers': 'localhost:' + SocketUtils.findAvailableTcpPort()]
        )

        when:
        KafkaHealthIndicator healthIndicator = applicationContext.getBean(KafkaHealthIndicator)
        HealthResult result = healthIndicator.result.next().block()

        then:
        result.status == DOWN
        result.details['error'] != null

        cleanup:
        applicationContext.close()
    }

    void "test kafka health indicator can be disabled"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(configuration +
                ['kafka.health.enabled': StringUtils.FALSE]
        )

        expect:
        !applicationContext.containsBean(KafkaHealthIndicator)

        cleanup:
        applicationContext.close()
    }
}

package io.micronaut.configuration.kafka.metrics

import io.micronaut.context.ApplicationContext
import spock.lang.Specification

class KafkaMetricsConfigurationSpec extends Specification {

    void "metric name style defaults to spring"() {
        given:
        ApplicationContext context = ApplicationContext.run()

        when:
        def configuration = context.getBean(KafkaMetricsConfigurationProperties)

        then:
        configuration.metricNameStyle == MetricNameStyle.SPRING

        cleanup:
        context.close()
    }

    void "metric name style can be configured"() {
        given:
        ApplicationContext context = ApplicationContext.run(
                (KafkaMetricsConfigurationProperties.PREFIX + ".metric-name-style"): "legacy"
        )

        when:
        def configuration = context.getBean(KafkaMetricsConfigurationProperties)

        then:
        configuration.metricNameStyle == MetricNameStyle.LEGACY

        cleanup:
        context.close()
    }
}

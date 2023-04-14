package io.micronaut.configuration.kafka.health


import io.micronaut.configuration.kafka.AbstractKafkaSpec
import io.micronaut.context.ApplicationContext
import io.micronaut.core.io.socket.SocketUtils
import io.micronaut.management.health.indicator.HealthResult
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.clients.admin.ConfigEntry
import spock.lang.Unroll

import static io.micronaut.configuration.kafka.health.KafkaHealthIndicator.DEFAULT_REPLICATION_PROPERTY
import static io.micronaut.configuration.kafka.health.KafkaHealthIndicator.REPLICATION_PROPERTY
import static io.micronaut.health.HealthStatus.DOWN
import static io.micronaut.health.HealthStatus.UP

class KafkaHealthIndicatorSpec extends AbstractKafkaSpec {

    void "test kafka health indicator - UP"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(configuration)

        when:
        KafkaHealthIndicator healthIndicator = applicationContext.getBean(KafkaHealthIndicator)
        HealthResult result = healthIndicator.result.next().block()

        then:
        // report down because the not enough nodes to meet replication factor
        result.status == UP
        result.details.nodes == 1

        cleanup:
        applicationContext.close()
    }

    void "test kafka health indicator - DOWN"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(configuration +
                ['kafka.bootstrap.servers': 'localhost:' + SocketUtils.findAvailableTcpPort()]
        )

        when:
        KafkaHealthIndicator healthIndicator = applicationContext.getBean(KafkaHealthIndicator)
        HealthResult result = healthIndicator.result.next().block()

        then:
        // report down because the not enough nodes to meet replication factor
        result.status == DOWN

        cleanup:
        applicationContext.close()
    }

    @Unroll
    void "test kafka health indicator - disabled (#configvalue)"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(configuration +
                ["kafka.health.enabled": configvalue]
        )

        when:
        Optional<KafkaHealthIndicator> optional = applicationContext.findBean(KafkaHealthIndicator)

        then:
        !optional.isPresent()

        cleanup:
        applicationContext.close()

        where:
        configvalue << [false, "false", "no", ""]
    }

    @Unroll
    void "kafka health indicator handle missing replication factor config"() {
        given:
        Collection<ConfigEntry> configEntries = []
        if (offsetFactor) {
            configEntries << new ConfigEntry(REPLICATION_PROPERTY, offsetFactor)
        }
        if (defaultFactor) {
            configEntries << new ConfigEntry(DEFAULT_REPLICATION_PROPERTY, defaultFactor)
        }
        Config config = new Config(configEntries)

        when:
        int replicationFactor = KafkaHealthIndicator.getClusterReplicationFactor(config)

        then:
        replicationFactor == expected

        where:
        offsetFactor | defaultFactor | expected
        "10"         | null          | 10
        "10"         | "8"           | 10
        null         | "8"           | 8
        null         | null          | Integer.MAX_VALUE
    }
}

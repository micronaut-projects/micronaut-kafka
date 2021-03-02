package io.micronaut.configuration.kafka.health

import io.micronaut.context.ApplicationContext
import io.micronaut.core.io.socket.SocketUtils
import io.micronaut.core.util.CollectionUtils
import io.micronaut.management.health.indicator.HealthResult
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.clients.admin.ConfigEntry
import org.testcontainers.containers.KafkaContainer
import spock.lang.Specification
import spock.lang.Unroll

import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED
import static io.micronaut.health.HealthStatus.DOWN
import static io.micronaut.health.HealthStatus.UP

class KafkaHealthIndicatorSpec extends Specification {

    void "test kafka health indicator - UP"() {
        given:
        KafkaContainer kafkaContainer = new KafkaContainer()
        kafkaContainer.start()
        ApplicationContext applicationContext = ApplicationContext.run(
                "kafka.bootstrap.servers": kafkaContainer.getBootstrapServers()
        )

        when:
        KafkaHealthIndicator healthIndicator = applicationContext.getBean(KafkaHealthIndicator)
        HealthResult result = healthIndicator.result.firstElement().blockingGet()

        then:
        // report down because the not enough nodes to meet replication factor
        result.status == UP
        result.details.nodes == 1

        cleanup:
        applicationContext.close()
        kafkaContainer.stop()
    }

    void "test kafka health indicator - DOWN"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                'kafka.bootstrap.servers': 'localhost:' + SocketUtils.findAvailableTcpPort()
        )

        when:
        KafkaHealthIndicator healthIndicator = applicationContext.getBean(KafkaHealthIndicator)
        HealthResult result = healthIndicator.result.firstElement().blockingGet()

        then:
        // report down because the not enough nodes to meet replication factor
        result.status == DOWN

        cleanup:
        applicationContext.close()
    }

    @Unroll
    void "test kafka health indicator - disabled (#configvalue)"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                CollectionUtils.mapOf(
                        EMBEDDED, true,
                        "kafka.health.enabled", configvalue)
        )

        when:
        def optional = applicationContext.findBean(KafkaHealthIndicator)

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
        if (offsetFactor) { configEntries.add(new ConfigEntry(KafkaHealthIndicator.REPLICATION_PROPERTY, offsetFactor)) }
        if (defaultFactor) { configEntries.add(new ConfigEntry(KafkaHealthIndicator.DEFAULT_REPLICATION_PROPERTY, defaultFactor)) }
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

package io.micronaut.configuration.kafka.health

import io.micronaut.configuration.kafka.config.KafkaDefaultConfiguration
import io.micronaut.context.ApplicationContext
import io.micronaut.core.io.socket.SocketUtils
import io.micronaut.management.health.indicator.HealthResult
import io.micronaut.testcontainers.kafka.Kafka
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.clients.admin.ConfigEntry
import spock.lang.Specification
import spock.lang.Unroll

import static io.micronaut.configuration.kafka.health.KafkaHealthIndicator.MIN_INSYNC_REPLICAS_PROPERTY
import static io.micronaut.configuration.kafka.health.KafkaHealthIndicator.REPLICATION_PROPERTY
import static io.micronaut.configuration.kafka.health.KafkaHealthIndicator.DEFAULT_REPLICATION_PROPERTY
import static io.micronaut.health.HealthStatus.DOWN
import static io.micronaut.health.HealthStatus.UP

class KafkaHealthIndicatorSpec extends Specification {

    Map<String, Object> getBaseConfig() {
        Kafka.getProperties() + ["spec.name": "KafkaHealthIndicatorSpec"]
    }

    void "test kafka health indicator - UP"() {
        given:
        ApplicationContext ctx = ApplicationContext.run(getBaseConfig())

        when:
        KafkaHealthIndicator healthIndicator = ctx.getBean(KafkaHealthIndicator)
        HealthResult result = healthIndicator.result.next().block()

        then:
        result.status == UP
        result.details.nodes == 1

        cleanup:
        ctx.close()
    }

    void "test kafka health indicator - DOWN"() {
        given:
        Map config = getBaseConfig()
        config["kafka.bootstrap.servers"] = "localhost:${SocketUtils.findAvailableTcpPort()}"
        ApplicationContext ctx = ApplicationContext.run(config)

        when:
        KafkaHealthIndicator healthIndicator = ctx.getBean(KafkaHealthIndicator)
        HealthResult result = healthIndicator.result.next().block()

        then:
        result.status == DOWN

        cleanup:
        ctx.close()
    }

    @Unroll
    void "test kafka health indicator - disabled (#configvalue)"() {
        given:
        Map config = getBaseConfig()
        config["kafka.health.enabled"] = configvalue
        ApplicationContext ctx = ApplicationContext.run(config)

        when:
        Optional<KafkaHealthIndicator> optional = ctx.findBean(KafkaHealthIndicator)

        then:
        !optional.isPresent()

        cleanup:
        ctx.close()

        where:
        configvalue << [false, "false", "no"]
    }

    void "test kafka health indicator - disabled when no kafka configuration provided"() {
        given:
        ApplicationContext ctx = ApplicationContext.run(["spec.name": "KafkaHealthIndicatorSpec"])

        when:
        Optional<KafkaDefaultConfiguration> config = ctx.findBean(KafkaDefaultConfiguration)
        Optional<KafkaHealthIndicator> healthIndicator = ctx.findBean(KafkaHealthIndicator)

        then:
        config.isEmpty()
        healthIndicator.isEmpty()

        cleanup:
        ctx.close()
    }

    @Unroll
    void "kafka health indicator handle missing replication factor config"() {
        given:
        Collection<ConfigEntry> configEntries = []
        if (minReplicas) {
            configEntries << new ConfigEntry(MIN_INSYNC_REPLICAS_PROPERTY, minReplicas)
        }
        if (offsetFactor) {
            configEntries << new ConfigEntry(REPLICATION_PROPERTY, offsetFactor)
        }
        if (defaultFactor) {
            configEntries << new ConfigEntry(DEFAULT_REPLICATION_PROPERTY, defaultFactor)
        }
        Config config = new Config(configEntries)

        when:
        int replicationFactor = KafkaHealthIndicator.getMinNodeCount(config)

        then:
        replicationFactor == expected

        where:
        minReplicas | offsetFactor | defaultFactor | expected
        "100"       | "10"         | null          | 100
        "100"       | "10"         | "8"           | 100
        "100"       | null         | "8"           | 100
        "100"       | null         | null          | 100
        null        | "10"         | null          | 10
        null        | "10"         | "8"           | 10
        null        | null         | "8"           | 8
        null        | null         | null          | Integer.MAX_VALUE
    }
}

package io.micronaut.configuration.kafka.streams.health

import io.micronaut.configuration.kafka.streams.AbstractTestContainersSpec
import io.micronaut.configuration.kafka.streams.KafkaStreamsFactory
import io.micronaut.management.health.aggregator.DefaultHealthAggregator
import io.micronaut.management.health.aggregator.HealthAggregator
import io.micronaut.management.health.indicator.HealthResult
import io.micronaut.runtime.ApplicationConfiguration
import org.apache.kafka.streams.KafkaStreams
import reactor.core.publisher.Mono
import spock.lang.Retry

import static io.micronaut.health.HealthStatus.UP

@Retry
class KafkaStreamsHealthSpec extends AbstractTestContainersSpec {

    void "should create object"() {
        given:
        def kafkaStreamsFactory = context.getBean(KafkaStreamsFactory)
        HealthAggregator healthAggregator = new DefaultHealthAggregator(Mock(ApplicationConfiguration))

        when:
        KafkaStreamsHealth kafkaStreamsHealth = new KafkaStreamsHealth(kafkaStreamsFactory, healthAggregator)

        then:
        kafkaStreamsHealth
    }

    void "should check health"() {
        given:
        def streamsHealth = context.getBean(KafkaStreamsHealth)

        expect:
        conditions.eventually {
            Mono.from(streamsHealth.result).block().status == UP
        }

        and:
        def healthLevelOne = Mono.from(streamsHealth.result).block()
        ((Map<String, HealthResult>) healthLevelOne.details).find { it.key == "micronaut-kafka-streams" }
        HealthResult healthLevelTwo = ((Map<String, HealthResult>) healthLevelOne.details).find { it.key == "micronaut-kafka-streams" }?.value
        healthLevelTwo.name == "micronaut-kafka-streams"
        healthLevelTwo.status == UP
        (healthLevelTwo.details as Map).size() == 8
        (healthLevelTwo.details as Map).containsKey("adminClientId")
        (healthLevelTwo.details as Map).containsKey("restoreConsumerClientId")
        (healthLevelTwo.details as Map).containsKey("threadState")
        (healthLevelTwo.details as Map).containsKey("producerClientIds")
        (healthLevelTwo.details as Map).containsKey("standbyTasks")
        (healthLevelTwo.details as Map).containsKey("activeTasks")
        (healthLevelTwo.details as Map).containsKey("consumerClientId")
        (healthLevelTwo.details as Map).containsKey("threadName")
    }

    void "test default if empty kafkaStream name"() {
        given:
        def streamsHealth = context.getBean(KafkaStreamsHealth)
        KafkaStreams kafkaStreams = context.getBeansOfType(KafkaStreams).first()

        expect:
        conditions.eventually {
            Mono.from(streamsHealth.result).block().status == UP
        }

        and:
        String name = KafkaStreamsHealth.getDefaultStreamName(kafkaStreams)
        name =~ "StreamThread"
    }

    void "test default if thread stopped"() {
        when:
        def streamsHealth = context.getBean(KafkaStreamsHealth)
        KafkaStreams kafkaStreams = context.getBeansOfType(KafkaStreams).first()

        then:
        conditions.eventually {
            Mono.from(streamsHealth.result).block().status == UP
        }

        when:
        kafkaStreams.close()

        then:
        String name = KafkaStreamsHealth.getDefaultStreamName(kafkaStreams)
        name =~ "unidentified"
    }

    void "test default null kafkaStream"() {
        expect:
        String name = KafkaStreamsHealth.getDefaultStreamName(null)
        name == "unidentified"
    }
}

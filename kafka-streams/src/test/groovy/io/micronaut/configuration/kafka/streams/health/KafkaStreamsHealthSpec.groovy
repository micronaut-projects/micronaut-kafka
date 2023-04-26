package io.micronaut.configuration.kafka.streams.health

import io.micronaut.configuration.kafka.streams.AbstractTestContainersSpec
import io.micronaut.configuration.kafka.streams.KafkaStreamsFactory
import io.micronaut.management.health.aggregator.DefaultHealthAggregator
import io.micronaut.management.health.aggregator.HealthAggregator
import io.micronaut.management.health.indicator.HealthResult
import io.micronaut.runtime.ApplicationConfiguration
import org.apache.kafka.streams.KafkaStreams
import reactor.core.publisher.Mono

import static io.micronaut.health.HealthStatus.UP

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

        Map<String, Map<String, String>> threadsDetails = healthLevelTwo.details as Map<String, Map<String, String>>
        threadsDetails.size() == 1

        Map<String, String> threadDetails = threadsDetails.values().first()
        threadDetails.size() == 8
        threadDetails.containsKey("adminClientId")
        threadDetails.containsKey("restoreConsumerClientId")
        threadDetails.containsKey("threadState")
        threadDetails.containsKey("producerClientIds")
        threadDetails.containsKey("standbyTasks")
        threadDetails.containsKey("activeTasks")
        threadDetails.containsKey("consumerClientId")
        threadDetails.containsKey("threadName")
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

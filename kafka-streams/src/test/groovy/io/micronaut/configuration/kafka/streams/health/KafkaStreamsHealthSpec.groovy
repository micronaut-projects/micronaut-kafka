package io.micronaut.configuration.kafka.streams.health

import io.micronaut.configuration.kafka.streams.AbstractTestContainersSpec
import io.micronaut.configuration.kafka.streams.KafkaStreamsFactory
import io.micronaut.management.health.aggregator.HealthAggregator
import io.micronaut.management.health.aggregator.RxJavaHealthAggregator
import io.micronaut.management.health.indicator.HealthResult
import io.micronaut.runtime.ApplicationConfiguration
import io.reactivex.Single
import org.apache.kafka.streams.KafkaStreams
import spock.lang.Retry

import static io.micronaut.health.HealthStatus.UP

@Retry
class KafkaStreamsHealthSpec extends AbstractTestContainersSpec {

    def "should create object"() {
        given:
        def kafkaStreamsFactory = embeddedServer.getApplicationContext().getBean(KafkaStreamsFactory)
        HealthAggregator healthAggregator = new RxJavaHealthAggregator(Mock(ApplicationConfiguration))

        when:
        KafkaStreamsHealth kafkaStreamsHealth = new KafkaStreamsHealth(kafkaStreamsFactory, healthAggregator)

        then:
        kafkaStreamsHealth
    }

    def "should check health"() {
        given:
        def streamsHealth = embeddedServer.getApplicationContext().getBean(KafkaStreamsHealth)

        expect:
        conditions.eventually {
            Single.fromPublisher(streamsHealth.getResult()).blockingGet().status == UP
        }

        and:
        def healthLevelOne = Single.fromPublisher(streamsHealth.getResult()).blockingGet()
        assert ((HashMap<String, HealthResult>) healthLevelOne.details).find { it.key == "micronaut-kafka-streams" }
        HealthResult healthLevelTwo = ((HashMap<String, HealthResult>) healthLevelOne.details).find { it.key == "micronaut-kafka-streams" }?.value
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

    def "test default if empty kafkaStream name"() {
        given:
        def streamsHealth = embeddedServer.getApplicationContext().getBean(KafkaStreamsHealth)
        KafkaStreams kafkaStreams = embeddedServer.getApplicationContext().getBeansOfType(KafkaStreams).first()

        expect:
        conditions.eventually {
            Single.fromPublisher(streamsHealth.getResult()).blockingGet().status == UP
        }

        and:
        def name = KafkaStreamsHealth.getDefaultStreamName(kafkaStreams)
        name =~ "StreamThread"
    }

    def "test default if thread stopped"() {
        when:
        def streamsHealth = embeddedServer.getApplicationContext().getBean(KafkaStreamsHealth)
        KafkaStreams kafkaStreams = embeddedServer.getApplicationContext().getBeansOfType(KafkaStreams).first()

        then:
        conditions.eventually {
            Single.fromPublisher(streamsHealth.getResult()).blockingGet().status == UP
        }

        when:
        kafkaStreams.close()

        then:
        def name = KafkaStreamsHealth.getDefaultStreamName(kafkaStreams)
        name =~ "unidentified"
    }

    def "test default null kafkaStream"() {
        expect:
        def name = KafkaStreamsHealth.getDefaultStreamName(null)
        name == "unidentified"
    }
}

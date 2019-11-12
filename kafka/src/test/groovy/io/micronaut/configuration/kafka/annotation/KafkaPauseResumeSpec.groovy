package io.micronaut.configuration.kafka.annotation

import io.micronaut.configuration.kafka.ConsumerRegistry
import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.CollectionUtils
import io.micronaut.messaging.annotation.Body
import io.micronaut.runtime.server.EmbeddedServer
import org.apache.kafka.clients.consumer.Consumer
import org.testcontainers.containers.KafkaContainer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.ConcurrentSkipListSet

class KafkaPauseResumeSpec extends Specification {

    @Shared @AutoCleanup KafkaContainer kafkaContainer = new KafkaContainer()

    @Shared
    @AutoCleanup
    EmbeddedServer embeddedServer

    @Shared
    @AutoCleanup
    ApplicationContext context

    def setupSpec() {
        kafkaContainer.start()
        embeddedServer = ApplicationContext.run(EmbeddedServer,
                CollectionUtils.mapOf(
                        "kafka.bootstrap.servers", kafkaContainer.getBootstrapServers(),
                        "micrometer.metrics.enabled", true,
                        'endpoints.metrics.sensitive', false,
                        AbstractKafkaConfiguration.EMBEDDED_TOPICS, ["fruits"]
                )
        )
        context = embeddedServer.applicationContext
    }

    void "test pause / resume listener"() {
        given:
        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)
        ConsumerRegistry registry = context.getBean(ConsumerRegistry)
        FruitClient client = context.getBean(FruitClient)
        FruitListener listener = context.getBean(FruitListener)

        when:
        Consumer consumer = registry.getConsumer("fruit-client")

        then:
        consumer != null
        registry.getConsumerIds()
        !registry.isPaused("fruit-client")

        when:
        client.send("test", "Apple")

        then:
        conditions.eventually {
            listener.fruits.size() == 1
        }

        when:"the consumer is paused"
        registry.pause("fruit-client")

        then:
        conditions.eventually {
            registry.isPaused("fruit-client")
        }

        when:
        client.send("test", "Banana")
        sleep(5000)

        then:
        listener.fruits.size() == 1

        when:
        registry.resume("fruit-client")

        then:
        conditions.eventually {
            !registry.isPaused("fruit-client")
            listener.fruits.size() == 2
        }
    }


    @KafkaClient
    static interface FruitClient {

        @Topic("fruits")
        void send(@KafkaKey String company, @Body String fruit)
    }

    @KafkaListener(clientId = "fruit-client", offsetReset = OffsetReset.EARLIEST)
    static class FruitListener {

        Set<String> fruits = new ConcurrentSkipListSet<>()

        @Topic("fruits")
        void receive(@Body String name) {
            println "RECEIVED FRUIT $name"
            fruits.add(name)
        }
    }
}

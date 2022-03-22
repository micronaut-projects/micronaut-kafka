package io.micronaut.configuration.kafka.annotation

import io.micronaut.configuration.kafka.AbstractEmbeddedServerSpec
import io.micronaut.configuration.kafka.ConsumerRegistry
import io.micronaut.context.annotation.Requires
import io.micronaut.messaging.annotation.MessageBody
import org.apache.kafka.clients.consumer.Consumer

import java.util.concurrent.ConcurrentSkipListSet

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

class KafkaPauseResumeSpec extends AbstractEmbeddedServerSpec {

    protected Map<String, Object> getConfiguration() {
        super.configuration +
                ['micrometer.metrics.enabled' : true,
                 'endpoints.metrics.sensitive': false,
                 (EMBEDDED_TOPICS)            : ['fruits']]
    }

    void "test pause / resume listener"() {
        given:
        ConsumerRegistry registry = context.getBean(ConsumerRegistry)
        FruitClient client = context.getBean(FruitClient)
        FruitListener listener = context.getBean(FruitListener)

        when:
        Consumer consumer = registry.getConsumer("fruit-client")

        then:
        consumer
        registry.consumerIds
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

        then:
        conditions.eventually {
            listener.fruits.size() == 1
        }

        when:
        registry.resume("fruit-client")

        then:
        conditions.eventually {
            !registry.isPaused("fruit-client")
            listener.fruits.size() == 2
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaPauseResumeSpec')
    @KafkaClient
    static interface FruitClient {
        @Topic("fruits")
        void send(@KafkaKey String company, @MessageBody String fruit)
    }

    @Requires(property = 'spec.name', value = 'KafkaPauseResumeSpec')
    @KafkaListener(clientId = "fruit-client", offsetReset = EARLIEST)
    static class FruitListener {

        Set<String> fruits = new ConcurrentSkipListSet<>()

        @Topic("fruits")
        void receive(@MessageBody String name) {
            fruits << name
        }
    }
}

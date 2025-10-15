package io.micronaut.configuration.kafka.annotation

import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import io.micronaut.configuration.kafka.exceptions.KafkaListenerExceptionHandler
import io.micronaut.context.annotation.Requires
import jakarta.inject.Inject
import org.apache.kafka.common.errors.SerializationException
import spock.lang.Stepwise

import java.util.concurrent.ConcurrentHashMap

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

@Stepwise
class KafkaTypeConversionSpec extends AbstractKafkaContainerSpec {

    protected Map<String, Object> getConfiguration() {
        super.configuration +
                [(EMBEDDED_TOPICS): ['uuids']]
    }

    void "test send valid UUID key"() {
        when:
        MyClient myClient = context.getBean(MyClient)
        UUID uuid = UUID.randomUUID()
        myClient.send(uuid.toString(), "test")

        MyListener myConsumer = context.getBean(MyListener)

        then:
        conditions.eventually {
            myConsumer.messages[uuid] == 'test'
        }
    }

    void "test send invalid UUID key"() {
        when:
        MyClient myClient = context.getBean(MyClient)
        String uuid = "bunch 'o junk"
        myClient.send(uuid, "test")

        MyListener myConsumer = context.getBean(MyListener)

        then:
        conditions.eventually {
            myConsumer.lastException != null
            myConsumer.lastException.cause instanceof SerializationException
            myConsumer.lastException.cause.printStackTrace()
            myConsumer.lastException.cause.message.contains("Error deserializing KEY for partition")
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaTypeConversionSpec')
    @KafkaListener(groupId = "MyUuidGroup", offsetReset = EARLIEST)
    static class MyListener implements KafkaListenerExceptionHandler {

        Map<UUID, String> messages = new ConcurrentHashMap<>()
        KafkaListenerException lastException

        @Inject
        KafkaListenerExceptionHandler defaultExceptionHandler

        @Topic("uuids")
        void receive(@KafkaKey UUID key, String message) {
            messages[key] = message
        }

        @Override
        void handle(KafkaListenerException exception) {
            lastException = exception
            defaultExceptionHandler.handle(exception)
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaTypeConversionSpec')
    @KafkaClient
    static interface MyClient {
        @Topic("uuids")
        void send(@KafkaKey String key, String message)
    }
}

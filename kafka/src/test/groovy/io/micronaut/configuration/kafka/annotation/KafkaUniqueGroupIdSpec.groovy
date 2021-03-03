package io.micronaut.configuration.kafka.annotation

import io.micronaut.configuration.kafka.AbstractEmbeddedServerSpec
import io.micronaut.context.annotation.Requires
import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.micronaut.http.client.RxHttpClient
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Stepwise

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

@Stepwise
class KafkaUniqueGroupIdSpec extends AbstractEmbeddedServerSpec {

    static final String TOPIC = "groupid-topic"

    @Shared @AutoCleanup RxHttpClient httpClient

    protected Map<String, Object> getConfiguration() {
        super.configuration +
                [(EMBEDDED_TOPICS): [KafkaUniqueGroupIdSpec.TOPIC]]
    }

    def setupSpec() {
        httpClient = context.createBean(
                RxHttpClient,
                embeddedServer.getURL(),
                new DefaultHttpClientConfiguration(followRedirects: false))
    }

    void "test multiple consumers - single group id"() {
        given:
        MyClient myClient = context.getBean(MyClient)
        MyConsumer1_1 myConsumer1_1 = context.getBean(MyConsumer1_1)
        MyConsumer1_2 myConsumer1_2 = context.getBean(MyConsumer1_2)

        when:
        myClient.sendMessage("key", "Test message 1")

        then:
        conditions.eventually {
            (myConsumer1_1.lastMessage == 'Test message 1' && myConsumer1_2.lastMessage == null) ||
            (myConsumer1_1.lastMessage == null && myConsumer1_2.lastMessage == 'Test message 1')

            myConsumer1_1.count + myConsumer1_2.count == 1
        }
    }

    void "test multiple consumers - multiple unique group ids - group id defined"() {
        given:
        MyClient myClient = context.getBean(MyClient)
        MyConsumer2_1 myConsumer2_1 = context.getBean(MyConsumer2_1)
        MyConsumer2_2 myConsumer2_2 = context.getBean(MyConsumer2_2)

        when:
        myClient.sendMessage("key", "Test message 2")

        then:
        conditions.eventually {
            (myConsumer2_1.lastMessage == 'Test message 2' && myConsumer2_2.lastMessage == 'Test message 2')
            myConsumer2_1.count + myConsumer2_2.count == 4 // Twice 'Test message 1' + twice 'Test message 2'
        }
    }

    void "test multiple consumers - multiple unique group ids - group id not defined"() {
        given:
        MyClient myClient = context.getBean(MyClient)
        MyConsumer3_1 myConsumer3_1 = context.getBean(MyConsumer3_1)
        MyConsumer3_2 myConsumer3_2 = context.getBean(MyConsumer3_2)

        when:
        myClient.sendMessage("key", "Test message 3")

        then:
        conditions.eventually {
            (myConsumer3_1.lastMessage == 'Test message 3' && myConsumer3_2.lastMessage == 'Test message 3')
            myConsumer3_1.count + myConsumer3_2.count == 6 // Twice 3 test messages
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaUniqueGroupIdSpec')
    @KafkaClient
    static interface MyClient {
        @Topic(KafkaUniqueGroupIdSpec.TOPIC)
        void sendMessage(@KafkaKey String key, String message)
    }

    @Requires(property = 'spec.name', value = 'KafkaUniqueGroupIdSpec')
    @KafkaListener(groupId = "myGroup", offsetReset = EARLIEST)
    static class MyConsumer1_1 {
        int count = 0
        String lastMessage

        @Topic(KafkaUniqueGroupIdSpec.TOPIC)
        void receiveMessage(@KafkaKey String key, String message) {
            lastMessage = message
            count++
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaUniqueGroupIdSpec')
    @KafkaListener(groupId = "myGroup", uniqueGroupId = false, offsetReset = EARLIEST)
    static class MyConsumer1_2 {
        int count = 0
        String lastMessage

        @Topic(KafkaUniqueGroupIdSpec.TOPIC)
        void receiveMessage(@KafkaKey String key, String message) {
            lastMessage = message
            count++
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaUniqueGroupIdSpec')
    @KafkaListener(groupId = "myGroup", uniqueGroupId = true, offsetReset = EARLIEST)
    static class MyConsumer2_1 {
        int count = 0
        String lastMessage

        @Topic(KafkaUniqueGroupIdSpec.TOPIC)
        void receiveMessage(@KafkaKey String key, String message) {
            lastMessage = message
            count++
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaUniqueGroupIdSpec')
    @KafkaListener(groupId = "myGroup", uniqueGroupId = true, offsetReset = EARLIEST)
    static class MyConsumer2_2 {
        int count = 0
        String lastMessage

        @Topic(KafkaUniqueGroupIdSpec.TOPIC)
        void receiveMessage(@KafkaKey String key, String message) {
            lastMessage = message
            count++
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaUniqueGroupIdSpec')
    @KafkaListener(/* No group ID defined */ uniqueGroupId = true, offsetReset = EARLIEST)
    static class MyConsumer3_1 {
        int count = 0
        String lastMessage

        @Topic(KafkaUniqueGroupIdSpec.TOPIC)
        void receiveMessage(@KafkaKey String key, String message) {
            lastMessage = message
            count++
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaUniqueGroupIdSpec')
    @KafkaListener(/* No group ID defined */ uniqueGroupId = true, offsetReset = EARLIEST)
    static class MyConsumer3_2 {
        int count = 0
        String lastMessage

        @Topic(KafkaUniqueGroupIdSpec.TOPIC)
        void receiveMessage(@KafkaKey String key, String message) {
            lastMessage = message
            count++
        }
    }
}

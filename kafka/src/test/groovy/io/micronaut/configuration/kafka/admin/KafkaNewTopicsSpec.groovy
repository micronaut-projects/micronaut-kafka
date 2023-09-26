package io.micronaut.configuration.kafka.admin

import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import org.apache.kafka.clients.admin.CreateTopicsOptions
import org.apache.kafka.clients.admin.NewTopic
import spock.util.concurrent.PollingConditions

class KafkaNewTopicsSpec extends AbstractKafkaContainerSpec {

    final static TOPIC_1 = 'new-topic-1'
    final static TOPIC_2 = 'new-topic-2'
    final static TOPIC_3 = '$illegal/topic:name!'

    void "create kafka topics"() {
        given:
        final KafkaNewTopics kafkaNewTopics = context.getBean(KafkaNewTopics)

        expect:
        kafkaNewTopics.result.isPresent()

        and:
        final result = kafkaNewTopics.result.get()
        new PollingConditions(timeout: 10).eventually {
            result.all().done == true
        }
        result.numPartitions(TOPIC_1).get() == 1
        result.numPartitions(TOPIC_2).get() == 2
        result.values()[TOPIC_3].completedExceptionally == true
    }

    @Requires(property = 'spec.name', value = 'KafkaNewTopicsSpec')
    @Factory
    static class MyTopicFactory {

        @Bean
        CreateTopicsOptions options() { new CreateTopicsOptions().timeoutMs(5000).validateOnly(true).retryOnQuotaViolation(false) }

        @Bean
        NewTopic topic1() { new NewTopic(TOPIC_1, Optional.of(1), Optional.empty()) }

        @Bean
        NewTopic topic2() { new NewTopic(TOPIC_2, Optional.of(2), Optional.empty()) }

        @Bean
        NewTopic topic3() { new NewTopic(TOPIC_3, Optional.of(4), Optional.empty()) }
    }
}

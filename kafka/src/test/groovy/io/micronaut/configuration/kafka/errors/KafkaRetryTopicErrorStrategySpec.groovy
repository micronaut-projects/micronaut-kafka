package io.micronaut.configuration.kafka.errors

import io.micronaut.configuration.kafka.AbstractEmbeddedServerSpec
import io.micronaut.configuration.kafka.annotation.ErrorStrategy
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import org.apache.kafka.clients.consumer.ConsumerRecord
import spock.lang.Shared

import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RETRY_TOPIC_ON_ERROR
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.SYNC

class KafkaRetryTopicErrorStrategySpec extends AbstractEmbeddedServerSpec {

    @Shared final String mainTopic = "errors-retry-topic-${UUID.randomUUID()}"
    @Shared final String retryTopic = "${mainTopic}-retry-100ms"
    @Shared final String dlqTopic = "${mainTopic}-dlq"

    @Override
    Map<String, Object> getConfiguration() {
        super.configuration + [
            'spec.retry.topic.main': mainTopic,
            'spec.retry.topic.dlq' : dlqTopic
        ]
    }

    @Override
    void afterKafkaStarted() {
        createTopic(mainTopic, 1, 1)
        createTopic(retryTopic, 1, 1)
        createTopic(dlqTopic, 1, 1)
    }

    void "failed records are retried on a retry topic without blocking later original records"() {
        when:
        RetryTopicClient client = context.getBean(RetryTopicClient)
        client.sendMessage('One')
        client.sendMessage('Two')

        RetryTopicConsumer consumer = context.getBean(RetryTopicConsumer)
        RetryTopicDlqConsumer dlqConsumer = context.getBean(RetryTopicDlqConsumer)

        then:
        conditions.eventually {
            consumer.successful == ["${mainTopic}:Two", "${retryTopic}:One"]
            consumer.retryAttempts == ['1']
            dlqConsumer.received.isEmpty()
        }
        and:
        consumer.retrySuccessTime - consumer.firstFailureTime >= 100
    }

    void "records are routed to the dead letter topic after retry topics are exhausted"() {
        when:
        RetryTopicClient client = context.getBean(RetryTopicClient)
        client.sendMessage("AlwaysFail")

        RetryTopicConsumer consumer = context.getBean(RetryTopicConsumer)
        RetryTopicDlqConsumer dlqConsumer = context.getBean(RetryTopicDlqConsumer)

        then:
        conditions.eventually {
            consumer.failedMessages.count("AlwaysFail") >= 2
            dlqConsumer.received == ["AlwaysFail"]
            dlqConsumer.originalTopics == [mainTopic]
        }
    }

    @Requires(property = 'spec.retry.topic.main')
    @KafkaClient
    static interface RetryTopicClient {
        @Topic('${spec.retry.topic.main}')
        void sendMessage(String message)
    }

    @Requires(property = 'spec.retry.topic.main')
    @KafkaListener(
        groupId = 'retry-topic-consumer',
        offsetReset = OffsetReset.EARLIEST,
        offsetStrategy = SYNC,
        errorStrategy = @ErrorStrategy(
            value = RETRY_TOPIC_ON_ERROR,
            retryTopicSuffixes = '-retry-100ms',
            retryTopicDelays = '100ms',
            dlq = '${spec.retry.topic.dlq}'
        )
    )
    static class RetryTopicConsumer {
        @Value('${spec.retry.topic.main}')
        String mainTopic

        final AtomicInteger failures = new AtomicInteger()
        final List<String> successful = new CopyOnWriteArrayList<>()
        final List<String> retryAttempts = new CopyOnWriteArrayList<>()
        final List<String> failedMessages = new CopyOnWriteArrayList<>()
        volatile long firstFailureTime
        volatile long retrySuccessTime

        @Topic('${spec.retry.topic.main}')
        void receive(ConsumerRecord<String, String> record) {
            failedMessages << record.value()
            if (record.value() == 'AlwaysFail') {
                if (firstFailureTime == 0) {
                    firstFailureTime = System.currentTimeMillis()
                }
                throw new IllegalStateException('always boom')
            }
            if (record.topic() == mainTopic && record.value() == 'One' && failures.incrementAndGet() == 1) {
                firstFailureTime = System.currentTimeMillis()
                throw new IllegalStateException('boom')
            }
            successful << "${record.topic()}:${record.value()}"
            if (record.topic() != mainTopic) {
                retrySuccessTime = System.currentTimeMillis()
                retryAttempts << new String(record.headers().lastHeader('micronaut-kafka-retry-attempt').value())
            }
        }
    }

    @Requires(property = 'spec.retry.topic.main')
    @KafkaListener(
        groupId = 'retry-topic-dlq-consumer',
        offsetReset = OffsetReset.EARLIEST,
        offsetStrategy = SYNC
    )
    static class RetryTopicDlqConsumer {
        final List<String> received = new CopyOnWriteArrayList<>()
        final List<String> originalTopics = new CopyOnWriteArrayList<>()

        @Topic('${spec.retry.topic.dlq}')
        void receive(String message, ConsumerRecord<String, String> record) {
            received << message
            originalTopics << new String(record.headers().lastHeader('micronaut-kafka-original-topic').value())
        }
    }
}

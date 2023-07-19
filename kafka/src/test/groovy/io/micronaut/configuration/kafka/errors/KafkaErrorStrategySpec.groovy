package io.micronaut.configuration.kafka.errors

import io.micronaut.configuration.kafka.AbstractEmbeddedServerSpec
import io.micronaut.configuration.kafka.annotation.ErrorStrategy
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import io.micronaut.configuration.kafka.exceptions.KafkaListenerExceptionHandler
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicInteger

import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.NONE
import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RESUME_AT_NEXT_RECORD
import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RETRY_EXPONENTIALLY_ON_ERROR
import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RETRY_ON_ERROR
import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.SYNC

class KafkaErrorStrategySpec extends AbstractEmbeddedServerSpec {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaErrorStrategySpec.class);
    private static final String RandomFailedMessage = (new Random().nextInt(30) + 10).toString();

    Map<String, Object> getConfiguration() {
        super.configuration +
                ["kafka.consumers.errors-retry-multiple-partitions.allow.auto.create.topics" : false]
    }

    @Override
    void afterKafkaStarted() {
        createTopic("errors-retry-multiple-partitions", 3, 1)
    }

    void "test when the error strategy is 'resume at next offset' the next message is consumed"() {
        when:"A consumer throws an exception"
        ResumeErrorClient myClient = context.getBean(ResumeErrorClient)
        myClient.sendMessage("One")
        myClient.sendMessage("Two")

        ResumeAtNextRecordErrorCausingConsumer myConsumer = context.getBean(ResumeAtNextRecordErrorCausingConsumer)

        then:"The message that threw the exception is skipped and the next message in the poll is processed"
        conditions.eventually {
            myConsumer.received == ["One", "Two"]
            myConsumer.count.get() == 2
        }
    }

    void "test when the error strategy is 'retry on error' the second message is not consumed"() {
        when:"A consumer throws an exception"
        RetryErrorClient myClient = context.getBean(RetryErrorClient)
        myClient.sendMessage("One")
        myClient.sendMessage("Two")

        RetryOnErrorErrorCausingConsumer myConsumer = context.getBean(RetryOnErrorErrorCausingConsumer)

        then:"The message that threw the exception is re-consumed"
        conditions.eventually {
            myConsumer.received == ["One", "One", "Two"]
            myConsumer.count.get() == 3
        }
        and:"the retry of the first message is delivered at least 50ms afterwards"
        myConsumer.times[1] - myConsumer.times[0] >= 50
    }

    void "test when the error strategy is 'retry on error' and retry failed, the finished messages should be complete except the failed message"() {
        when:"A client sends a lot of messages to a same topic"
        RetryErrorMultiplePartitionsClient myClient = context.getBean(RetryErrorMultiplePartitionsClient)

        var messages = new HashSet()
        for(int i = 0; i < 50; i++) {
            myClient.sendMessage(i.toString())
            messages.add(i.toString())
        }

        then:"The finished messages should be complete except the particularly failed one"
        RetryOnErrorMultiplePartitionsErrorCausingConsumer myConsumer = context.getBean(RetryOnErrorMultiplePartitionsErrorCausingConsumer)
        var expected = new HashSet();
        expected.add(RandomFailedMessage)
        conditions.eventually {
           messages - myConsumer.finished == expected
        }
    }

    void "test when error strategy is 'retry exponentially on error' then message is retried with exponential backoff"() {
        when: "A consumer throws an exception"
        ExpRetryErrorClient myClient = context.getBean(ExpRetryErrorClient)
        myClient.sendMessage("One")

        RetryExpOnErrorErrorCausingConsumer myConsumer = context.getBean(RetryExpOnErrorErrorCausingConsumer)

        then: "Message is consumed eventually"
        conditions.eventually {
            myConsumer.received == ["One", "One", "One", "One"]
            myConsumer.count.get() == 4
        }
        and: "message was retried with exponential breaks between deliveries"
        myConsumer.times[1] - myConsumer.times[0] >= 50
        myConsumer.times[2] - myConsumer.times[1] >= 100
        myConsumer.times[3] - myConsumer.times[2] >= 200
    }

    void "test simultaneous retry and consumer reassignment"() {
        when: "A consumer throws an exception"
        TimeoutAndRetryErrorClient myClient = context.getBean(TimeoutAndRetryErrorClient)
        myClient.sendMessage("One")
        myClient.sendMessage("Two")

        RetryAndRebalanceOnErrorErrorCausingConsumer myConsumer = context.getBean(RetryAndRebalanceOnErrorErrorCausingConsumer)

        then: "The message that threw the exception is re-consumed"
        conditions.eventually {
            myConsumer.received == ["One", "One", "Two"]
            myConsumer.finished == ["One", "Two", "One"]
            sleep(1000)  // wait for the sleeping consumer to wake up and misbehave
            myConsumer.exceptionCount.get() == 0
        }
        and:"the retry of the first message is delivered at least 5000ms afterwards"
        myConsumer.times[1] - myConsumer.times[0] >= 5_000
    }

    /**
     * @deprecated This test is deprecated as the poll next strategy is default to ensure backwards
     * compatibility with existing (broken) functionality that people may have workarounds for with
     * custom error handlers.
     */
    @Deprecated
    void "test an exception that is thrown is not committed with default error strategy"() {
        when:"A consumer throws an exception"
        PollNextErrorClient myClient = context.getBean(PollNextErrorClient)
        myClient.sendMessage("One")
        myClient.sendMessage("Two")

        PollNextErrorCausingConsumer myConsumer = context.getBean(PollNextErrorCausingConsumer)

        then:"The message is re-delivered and eventually handled"
        conditions.eventually {
            myConsumer.received.size() == 2
            myConsumer.count.get() == 3
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaListener(offsetReset = EARLIEST, offsetStrategy = SYNC, errorStrategy = @ErrorStrategy(value = RESUME_AT_NEXT_RECORD))
    static class ResumeAtNextRecordErrorCausingConsumer {
        AtomicInteger count = new AtomicInteger(0)
        List<String> received = []

        @Topic("errors-resume")
        void handleMessage(String message) {
            received << message
            if (count.getAndIncrement() == 0) {
                throw new RuntimeException("Won't handle first")
            }
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaListener(
        offsetReset = EARLIEST,
        offsetStrategy = SYNC,
        errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR, retryDelay = "50ms")
    )
    static class RetryOnErrorErrorCausingConsumer {
        AtomicInteger count = new AtomicInteger(0)
        List<String> received = []
        List<Long> times = []

        @Topic("errors-retry")
        void handleMessage(String message) {
            received << message
            times << System.currentTimeMillis()
            if (count.getAndIncrement() == 0) {
                throw new RuntimeException("Won't handle first")
            }
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaListener(
            value="errors-retry-multiple-partitions",
            offsetStrategy = SYNC,
            errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR)
    )
    static class RetryOnErrorMultiplePartitionsErrorCausingConsumer {
        AtomicInteger count = new AtomicInteger(0)
        Set<String> received = []
        Set<String> finished = []

        @Topic("errors-retry-multiple-partitions")
        void handleMessage(String message) {
            received << message
            count.getAndIncrement()
            if (message == RandomFailedMessage) {
                throw new RuntimeException("Won't handle the error message: " + RandomFailedMessage)
            }
            finished << message
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaListener(
        offsetReset = EARLIEST,
        offsetStrategy = SYNC,
        errorStrategy = @ErrorStrategy(value = RETRY_EXPONENTIALLY_ON_ERROR, retryCount = 3, retryDelay = "50ms")
    )
    static class RetryExpOnErrorErrorCausingConsumer {
        AtomicInteger count = new AtomicInteger(0)
        List<String> received = []
        List<Long> times = []

        @Topic("errors-exp-retry")
        void handleMessage(String message) {
            received << message
            times << System.currentTimeMillis()
            if (count.getAndIncrement() < 4) {
                throw new RuntimeException("Won't handle first three delivery attempts")
            }
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaListener(offsetReset = EARLIEST, offsetStrategy = SYNC, errorStrategy = @ErrorStrategy(value = NONE))
    static class PollNextErrorCausingConsumer implements KafkaListenerExceptionHandler {
        AtomicInteger count = new AtomicInteger(0)
        List<String> received = []

        @Topic("errors-poll")
        void handleMessage(String message) {
            if (count.getAndIncrement() == 1) {
                throw new RuntimeException("Won't handle first")
            }
            received << message
        }

        @Override
        void handle(KafkaListenerException exception) {
            def record = exception.consumerRecord.orElse(null)
            def consumer = exception.kafkaConsumer
            consumer.seek(
                    new TopicPartition("errors-poll", record.partition()),
                    record.offset()
            )
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaListener(
        offsetReset = EARLIEST,
        threads = 2,
        errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR),
        properties = @Property(name = ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, value = "5000")
    )
    static class RetryAndRebalanceOnErrorErrorCausingConsumer implements KafkaListenerExceptionHandler {
        AtomicInteger count = new AtomicInteger(0)
        AtomicInteger exceptionCount = new AtomicInteger(0)
        List<String> received = []
        List<String> finished = []
        List<Long> times = []

        @Topic("errors-timeout-and-retry")
        void handleMessage(String message) {
            LOG.info("Got {}", message)
            received << message
            times << System.currentTimeMillis()
            try {
                if (count.getAndIncrement() == 0) {
                    Thread.sleep(10_000)
                    throw new RuntimeException("Won't handle first")
                }
            } finally {
                // This lets us block the test conditions until the first thread (that sleeps) has a chance to wake back up
                finished << message
            }
        }

        @Override
        void handle(KafkaListenerException exception) {
            exceptionCount.getAndIncrement()
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaClient
    static interface ResumeErrorClient {
        @Topic("errors-resume")
        void sendMessage(String message)
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaClient
    static interface RetryErrorClient {
        @Topic("errors-retry")
        void sendMessage(String message)
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaClient
    static interface RetryErrorMultiplePartitionsClient {
        @Topic("errors-retry-multiple-partitions")
        void sendMessage(String message)
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaClient
    static interface ExpRetryErrorClient {
        @Topic("errors-exp-retry")
        void sendMessage(String message)
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaClient
    static interface PollNextErrorClient {
        @Topic("errors-poll")
        void sendMessage(String message)
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaClient
    static interface TimeoutAndRetryErrorClient {
        @Topic("errors-timeout-and-retry")
        void sendMessage(String message)
    }
}

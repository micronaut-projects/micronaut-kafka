package io.micronaut.configuration.kafka.errors

import io.micronaut.configuration.kafka.AbstractEmbeddedServerSpec
import io.micronaut.configuration.kafka.annotation.ErrorStrategy
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.retry.DefaultConditionalRetryBehaviourHandler
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import io.micronaut.configuration.kafka.exceptions.KafkaListenerExceptionHandler
import io.micronaut.configuration.kafka.retry.ConditionalRetryBehaviourHandler
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.core.annotation.Blocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import spock.lang.Unroll

import java.util.concurrent.atomic.AtomicInteger

import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.NONE
import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RESUME_AT_NEXT_RECORD
import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RETRY_CONDITIONALLY_ON_ERROR
import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RETRY_CONDITIONALLY_EXPONENTIALLY_ON_ERROR
import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RETRY_EXPONENTIALLY_ON_ERROR
import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RETRY_ON_ERROR
import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.SYNC

class KafkaErrorStrategySpec extends AbstractEmbeddedServerSpec {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaErrorStrategySpec.class);
    private static final String RandomFailedMessage = (new Random().nextInt(30) + 10).toString();

    Map<String, Object> getConfiguration() {
        super.configuration +
                ["kafka.consumers.errors-retry-multiple-partitions.allow.auto.create.topics" : false,
                 "my.retry.count": "3"]
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

    void "test when the error strategy is 'retry conditionally on error' messages can be conditionally skipped when errors occur"() {
        when:"A consumer throws an exception"
        ConditionallyRetryErrorClient myClient = context.getBean(ConditionallyRetryErrorClient)
        myClient.sendMessage("One")
        myClient.sendMessage("Two")
        myClient.sendMessage("Three")

        ConditionallyRetryOnErrorErrorCausingConsumer myConsumer = context.getBean(ConditionallyRetryOnErrorErrorCausingConsumer)

        then:"The message that threw the exception is re-consumed"
        conditions.eventually {
            myConsumer.received == ["One", "One", "Two", "Three"]
            myConsumer.successful == ["Three"]
            myConsumer.count.get() == 4
        }
        and:"the retry of the first message is delivered at least 50ms afterwards"
        myConsumer.times[1] - myConsumer.times[0] >= 50
        myConsumer.skipped == ["Two"]
    }

    void "test when the error strategy is 'retry conditionally on error' messages can be conditionally skipped when errors occur and no exceptions match"() {
        when:"A consumer throws an exception"
        ConditionallyRetryResultsInSkipWhenNoExceptionsMatchedErrorClient myClient = context.getBean(ConditionallyRetryResultsInSkipWhenNoExceptionsMatchedErrorClient)
        myClient.sendMessage("One")
        myClient.sendMessage("Two")
        myClient.sendMessage("Three")

        ConditionallyRetryOnErrorSkipRespectedWhenNoExceptionTypesMatchedErrorCausingConsumer myConsumer = context.getBean(ConditionallyRetryOnErrorSkipRespectedWhenNoExceptionTypesMatchedErrorCausingConsumer)

        then:"The second message was skipped"
        conditions.eventually {
            myConsumer.received == ["One", "One", "Two", "Three"]
            myConsumer.successful == ["Three"]
            myConsumer.count.get() == 4
            myConsumer.skipped == ["Two"]
        }
    }

    void "test when the error strategy is 'retry conditionally on error' conditionally skipped messages are overridden when errors occur and exceptions match"() {
        when:"A consumer throws an exception"
        ConditionallyRetryOnErrorSkipIsOverriddenWithRetryWhenExceptionsMatchErrorClient myClient = context.getBean(ConditionallyRetryOnErrorSkipIsOverriddenWithRetryWhenExceptionsMatchErrorClient)
        myClient.sendMessage("One")
        myClient.sendMessage("Two")
        myClient.sendMessage("Three")

        ConditionallyRetryOnErrorSkipOverriddenWhenExceptionTypesMatchedErrorCausingConsumer myConsumer = context.getBean(ConditionallyRetryOnErrorSkipOverriddenWhenExceptionTypesMatchedErrorCausingConsumer)

        then:"The second message was supposed to be skipped but was retried due to the exception type"
        conditions.eventually {
            myConsumer.received == ["One", "One", "Two", "Two", "Three"]
            myConsumer.successful == ["Three"]
            myConsumer.count.get() == 5
            myConsumer.skipped == ["Two", "Two"]
        }
    }

    void "test when the error strategy is 'retry on error' and there are serialization errors"() {
        when:"A record cannot be deserialized"
        DeserializationErrorClient myClient = context.getBean(DeserializationErrorClient)
        myClient.sendText("Not an integer")
        myClient.sendNumber(123)

        RetryOnErrorDeserializationErrorConsumer myConsumer = context.getBean(RetryOnErrorDeserializationErrorConsumer)

        then:"The message that threw the exception is eventually left behind"
        conditions.eventually {
            myConsumer.number == 123
        }
        and:"the retry error strategy is honored"
        myConsumer.exceptionCount.get() == 2
    }

    void "test when the error strategy is 'retry conditionally on error' messages are retried with the default seek behaviour when deserialization errors occur"() {
        when: "A record cannot be deserialized"
        ConditionalRetryAlwaysRetryDeserializationErrorClient myClient = context.getBean(ConditionalRetryAlwaysRetryDeserializationErrorClient)
        myClient.sendText("Not an integer and should be retried")
        myClient.sendNumber(123)

        ConditionallyRetryOnErrorAlwaysRetryDeserializationErrorConsumer myConsumer = context.getBean(ConditionallyRetryOnErrorAlwaysRetryDeserializationErrorConsumer)

        then: "The message that threw the exception is eventually left behind"
        conditions.eventually {
            myConsumer.number == 123
        }

        and: "the default conditional retry behaviour handler bean exists"
        ConditionalRetryBehaviourHandler conditionalRetryBehaviourHandler = context.getBean(ConditionalRetryBehaviourHandler.class)
        conditionalRetryBehaviourHandler instanceof DefaultConditionalRetryBehaviourHandler

        and: "the first message was retried"
        myConsumer.exceptionCount.get() == 2
    }

    void "test when the error strategy is 'retry conditionally on error' messages can be conditionally skipped when deserialization errors occur"() {
        when: "A record cannot be deserialized"
        ConditionalLogicInConsumerBeanDeserializationErrorClient myClient = context.getBean(ConditionalLogicInConsumerBeanDeserializationErrorClient)
        myClient.sendText("Not an integer and should be immediately skipped")
        myClient.sendText("Not an integer and should be retried")
        myClient.sendNumber(123)

        ConditionallyRetryOnErrorLogicInConsumerBeanDeserializationErrorConsumer myConsumer = context.getBean(ConditionallyRetryOnErrorLogicInConsumerBeanDeserializationErrorConsumer)

        then: "The messages that threw the exception are eventually left behind"
        conditions.eventually {
            myConsumer.number == 123
        }

        and: "the first message was only tried once and the second was tried twice"
        myConsumer.skippedMessagesCount.get() == 1
        myConsumer.exceptionCount.get() == 3
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

    void "test when error strategy is 'retry exponentially and conditionally on error' then message is retried with exponential backoff"() {
        when: "A consumer throws an exception"
        ExpAndConditionalRetryErrorClient myClient = context.getBean(ExpAndConditionalRetryErrorClient)
        myClient.sendMessage("One")
        myClient.sendMessage("Two")
        myClient.sendMessage("Three")

        RetryExpAndConditionallyOnErrorErrorCausingConsumer myConsumer = context.getBean(RetryExpAndConditionallyOnErrorErrorCausingConsumer)

        then: "First message retries and is eventually skipped, the second is immediately skipped, and the last is eventually consumed"
        conditions.eventually {
            myConsumer.received == ["One", "One", "One", "One", "Two", "Three"]
            myConsumer.successful == ["Three"]
            myConsumer.count.get() == 6
            myConsumer.skipped == ["Two"]
        }
        and: "message was retried with exponential breaks between deliveries"
        myConsumer.times[1] - myConsumer.times[0] >= 50
        myConsumer.times[2] - myConsumer.times[1] >= 100
        myConsumer.times[3] - myConsumer.times[2] >= 200
    }

    void "test when error strategy is 'retry on error' and 'handle all exceptions' is true"() {
        when: "A consumer throws an exception"
        RetryHandleAllErrorClient myClient = context.getBean(RetryHandleAllErrorClient)
        myClient.sendMessage("One")
        myClient.sendMessage("Two")
        myClient.sendMessage("Three")

        RetryHandleAllErrorCausingConsumer myConsumer = context.getBean(RetryHandleAllErrorCausingConsumer)

        then: "Messages are consumed eventually"
        conditions.eventually {
            myConsumer.received == ["One", "Two", "Two", "Three", "Three", "Three"]
            myConsumer.count.get() == 6
        }
        and: "messages were retried and all exceptions were handled"
        myConsumer.errors.size() == 4
        myConsumer.errors[0].message == "Two #2"
        myConsumer.errors[1].message == "Three #4"
        myConsumer.errors[2].message == "Three #5"
        myConsumer.errors[3].message == "Three #6"
    }

    void "test reactive consumer when error strategy is 'retry on error' and 'handle all exceptions' is true"() {
        when: "A reactive consumer signals an error"
        RetryReactiveHandleAllErrorClient myClient = context.getBean(RetryReactiveHandleAllErrorClient)
        myClient.sendMessage("One")
        myClient.sendMessage("Two")
        myClient.sendMessage("Three")

        RetryReactiveHandleAllErrorCausingConsumer myConsumer = context.getBean(RetryReactiveHandleAllErrorCausingConsumer)

        then: "Messages are consumed eventually"
        conditions.eventually {
            myConsumer.received == ["One", "Two", "Two", "Three", "Three", "Three"]
            myConsumer.count.get() == 6
        }
        and: "messages were retried and all exceptions were handled"
        myConsumer.errors.size() == 4
        myConsumer.errors[0].message == "Two #2"
        myConsumer.errors[1].message == "Three #4"
        myConsumer.errors[2].message == "Three #5"
        myConsumer.errors[3].message == "Three #6"
    }

    @Unroll
    void "test when error strategy is 'retry on error' with #type retry count"(String type) {
        when: "A consumer throws an exception"
        RetryCountClient myClient = context.getBean(RetryCountClient)
        myClient.sendMessage("${type}-retry-count", "ERROR")
        myClient.sendMessage("${type}-retry-count", "OK")

        AbstractRetryCountConsumer myConsumer = context.getBean(consumerClass)

        then: "Messages are consumed eventually"
        conditions.eventually {
            myConsumer.received == "OK"
        }
        and: "messages were retried the correct number of times"
        myConsumer.errors.size() == expectedErrorCount

        where:
        type      | consumerClass             | expectedErrorCount
        'fixed'   | FixedRetryCountConsumer   | 11
        'dynamic' | DynamicRetryCountConsumer | 4
        'mixed'   | MixedRetryCountConsumer   | 11
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
        offsetReset = EARLIEST,
        offsetStrategy = SYNC,
        errorStrategy = @ErrorStrategy(value = RETRY_CONDITIONALLY_ON_ERROR, retryDelay = "50ms")
    )
    static class ConditionallyRetryOnErrorErrorCausingConsumer implements ConditionalRetryBehaviourHandler {
        AtomicInteger count = new AtomicInteger(0)
        List<String> received = []
        List<Long> times = []
        List<String> skipped = []
        List<String> successful = []

        @Topic("errors-conditional-retry")
        void handleMessage(String message) {
            received << message
            times << System.currentTimeMillis()
            if (count.getAndIncrement() < 3) {
                throw new RuntimeException("Won't handle the first message and the first attempt of the second")
            }
            successful << message
        }

        @Override
        ConditionalRetryBehaviour conditionalRetryBehaviour(KafkaListenerException exception) {
            if (exception.consumerRecord.get().value() == "Two") {
                skipped << (String) exception.consumerRecord.get().value()
                return ConditionalRetryBehaviour.SKIP
            } else {
                return ConditionalRetryBehaviour.RETRY
            }
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaListener(
            offsetReset = EARLIEST,
            offsetStrategy = SYNC,
            errorStrategy = @ErrorStrategy(value = RETRY_CONDITIONALLY_ON_ERROR, retryDelay = "50ms", exceptionTypes = [IllegalArgumentException.class])
    )
    static class ConditionallyRetryOnErrorSkipRespectedWhenNoExceptionTypesMatchedErrorCausingConsumer implements ConditionalRetryBehaviourHandler {
        AtomicInteger count = new AtomicInteger(0)
        List<String> received = []
        List<Long> times = []
        List<String> skipped = []
        List<String> successful = []

        @Topic("errors-conditional-retry-skip-when-no-exceptions-matched")
        void handleMessage(String message) {
            received << message
            times << System.currentTimeMillis()
            if (count.getAndIncrement() < 3) {
                throw new RuntimeException("Won't handle the first message and the first attempt of the second")
            }
            successful << message
        }

        @Override
        ConditionalRetryBehaviour conditionalRetryBehaviour(KafkaListenerException exception) {
            if (exception.consumerRecord.get().value() == "Two") {
                skipped << (String) exception.consumerRecord.get().value()
                return ConditionalRetryBehaviour.SKIP
            } else {
                return ConditionalRetryBehaviour.RETRY
            }
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaListener(
            offsetReset = EARLIEST,
            offsetStrategy = SYNC,
            errorStrategy = @ErrorStrategy(value = RETRY_CONDITIONALLY_ON_ERROR, retryDelay = "50ms", exceptionTypes = [IllegalArgumentException.class])
    )
    static class ConditionallyRetryOnErrorSkipOverriddenWhenExceptionTypesMatchedErrorCausingConsumer implements ConditionalRetryBehaviourHandler {
        AtomicInteger count = new AtomicInteger(0)
        List<String> received = []
        List<Long> times = []
        List<String> skipped = []
        List<String> successful = []

        @Topic("errors-conditional-retry-skip-overriden-when-exceptions-matched")
        void handleMessage(String message) {
            received << message
            times << System.currentTimeMillis()
            count.getAndIncrement();
            if (message == "One") {
                throw new RuntimeException("Won't handle the first message")
            }
            if (message == "Two") {
                throw new IllegalArgumentException("Won't handle the second message")
            }
            successful << message
        }

        @Override
        ConditionalRetryBehaviour conditionalRetryBehaviour(KafkaListenerException exception) {
            if (exception.getCause() instanceof IllegalArgumentException) {
                skipped << (String) exception.consumerRecord.get().value()
                return ConditionalRetryBehaviour.SKIP
            } else {
                return ConditionalRetryBehaviour.RETRY
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
    @KafkaListener(
            offsetReset = EARLIEST,
            offsetStrategy = SYNC,
            errorStrategy = @ErrorStrategy(value = RETRY_CONDITIONALLY_EXPONENTIALLY_ON_ERROR, retryCount = 3, retryDelay = "50ms", handleAllExceptions = true)
    )
    static class RetryExpAndConditionallyOnErrorErrorCausingConsumer implements ConditionalRetryBehaviourHandler {
        AtomicInteger count = new AtomicInteger(0)
        List<String> received = []
        List<Long> times = []
        List<String> skipped = []
        List<String> successful = []

        @Topic("errors-exp-conditional-retry")
        void handleMessage(String message) {
            received << message
            times << System.currentTimeMillis()
            if (count.getAndIncrement() < 5) {
                throw new RuntimeException("Won't handle the first message and the first attempt of the second")
            }
            successful << message
        }

        @Override
        ConditionalRetryBehaviour conditionalRetryBehaviour(KafkaListenerException exception) {
            if (exception.consumerRecord.get().value() == "Two") {
                skipped << (String) exception.consumerRecord.get().value()
                return ConditionalRetryBehaviour.SKIP
            } else {
                return ConditionalRetryBehaviour.RETRY
            }
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaListener(
        offsetReset = EARLIEST,
        offsetStrategy = SYNC,
        errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR, retryCount = 2, handleAllExceptions = true)
    )
    static class RetryHandleAllErrorCausingConsumer implements KafkaListenerExceptionHandler {
        AtomicInteger count = new AtomicInteger(0)
        List<String> received = []
        List<KafkaListenerException> errors = []

        @Topic("errors-retry-handle-all-exceptions")
        void handleMessage(String message) {
            received << message
            if (count.getAndIncrement() == 1 || message == 'Three') {
                throw new RuntimeException("${message} #${count}")
            }
        }

        @Override
        void handle(KafkaListenerException exception) {
            errors << exception
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaListener(
            offsetReset = EARLIEST,
            offsetStrategy = SYNC,
            errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR, retryCount = 2, handleAllExceptions = true)
    )
    static class RetryReactiveHandleAllErrorCausingConsumer implements KafkaListenerExceptionHandler {
        AtomicInteger count = new AtomicInteger(0)
        List<String> received = []
        List<KafkaListenerException> errors = []

        @Blocking
        @Topic("errors-retry-reactive-handle-all-exceptions")
        Mono<Boolean> handleMessage(String message) {
            received << message
            if (count.getAndIncrement() == 1 || message == 'Three') {
                return Mono.error(new RuntimeException("${message} #${count}"))
            }
            return Mono.just(Boolean.TRUE)
        }

        @Override
        void handle(KafkaListenerException exception) {
            errors << exception
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
            value="errors-retry-deserialization-error",
            errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR, handleAllExceptions = true)
    )
    static class RetryOnErrorDeserializationErrorConsumer implements KafkaListenerExceptionHandler {
        int number = 0
        AtomicInteger exceptionCount = new AtomicInteger(0)

        @Topic("deserialization-errors-retry")
        void handleMessage(int number) {
            this.number = number
        }

        @Override
        void handle(KafkaListenerException exception) {
            exceptionCount.getAndIncrement()
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaListener(
            offsetReset = EARLIEST,
            errorStrategy = @ErrorStrategy(value = RETRY_CONDITIONALLY_ON_ERROR, handleAllExceptions = true)
    )
    static class ConditionallyRetryOnErrorAlwaysRetryDeserializationErrorConsumer implements KafkaListenerExceptionHandler {
        int number = 0
        AtomicInteger exceptionCount = new AtomicInteger(0)

        @Topic("errors-conditionally-retry-always-retry-deserialization-error")
        void handleMessage(int number) {
            this.number = number
        }

        @Override
        void handle(KafkaListenerException exception) {
            exceptionCount.getAndIncrement()
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaListener(
            offsetReset = EARLIEST,
            errorStrategy = @ErrorStrategy(value = RETRY_CONDITIONALLY_ON_ERROR, handleAllExceptions = true)
    )
    static class ConditionallyRetryOnErrorLogicInConsumerBeanDeserializationErrorConsumer implements ConditionalRetryBehaviourHandler, KafkaListenerExceptionHandler {
        int number = 0
        AtomicInteger exceptionCount = new AtomicInteger(0)
        AtomicInteger skippedMessagesCount = new AtomicInteger(0)

        @Topic("errors-conditionally-retry-logic-in-consumer-bean-deserialization-error")
        void handleMessage(int number) {
            this.number = number
        }

        @Override
        ConditionalRetryBehaviour conditionalRetryBehaviour(KafkaListenerException exception) {
            if (exception.consumerRecord.get().offset() == 0) {
                skippedMessagesCount.getAndIncrement()
                return ConditionalRetryBehaviour.SKIP
            }
            return ConditionalRetryBehaviour.RETRY
        }

        @Override
        void handle(KafkaListenerException exception) {
            exceptionCount.getAndIncrement()
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

    static abstract class AbstractRetryCountConsumer implements KafkaListenerExceptionHandler {
        List<KafkaListenerException> errors = []
        String received

        void receive(String message) {
            if (message == 'ERROR') throw new RuntimeException("Won't handle this one")
            received = message
        }

        @Override
        void handle(KafkaListenerException exception) {
            errors << exception
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaListener(
            offsetReset = EARLIEST,
            errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR, retryCount = 10, handleAllExceptions = true)
    )
    static class FixedRetryCountConsumer extends AbstractRetryCountConsumer {
        @Topic("fixed-retry-count")
        void receiveMessage(String message) {
            receive(message)
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaListener(
            offsetReset = EARLIEST,
            errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR, retryCountValue = '${my.retry.count}', handleAllExceptions = true)
    )
    static class DynamicRetryCountConsumer extends AbstractRetryCountConsumer {
        @Topic("dynamic-retry-count")
        void receiveMessage(String message) {
            receive(message)
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaListener(
            offsetReset = EARLIEST,
            errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR, retryCount = 10, retryCountValue = '${my.retry.count}', handleAllExceptions = true)
    )
    static class MixedRetryCountConsumer extends AbstractRetryCountConsumer {
        @Topic("mixed-retry-count")
        void receiveMessage(String message) {
            receive(message)
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
    static interface ConditionallyRetryErrorClient {
        @Topic("errors-conditional-retry")
        void sendMessage(String message)
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaClient
    static interface ConditionallyRetryResultsInSkipWhenNoExceptionsMatchedErrorClient {
        @Topic("errors-conditional-retry-skip-when-no-exceptions-matched")
        void sendMessage(String message)
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaClient
    static interface ConditionallyRetryOnErrorSkipIsOverriddenWithRetryWhenExceptionsMatchErrorClient {
        @Topic("errors-conditional-retry-skip-overriden-when-exceptions-matched")
        void sendMessage(String message)
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaClient
    static interface DeserializationErrorClient {
        @Topic("deserialization-errors-retry")
        void sendText(String text)

        @Topic("deserialization-errors-retry")
        void sendNumber(int number)
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaClient
    static interface ConditionalRetryAlwaysRetryDeserializationErrorClient {
        @Topic("errors-conditionally-retry-always-retry-deserialization-error")
        void sendText(String text)

        @Topic("errors-conditionally-retry-always-retry-deserialization-error")
        void sendNumber(int number)
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaClient
    static interface ConditionalLogicInConsumerBeanDeserializationErrorClient {
        @Topic("errors-conditionally-retry-logic-in-consumer-bean-deserialization-error")
        void sendText(String text)

        @Topic("errors-conditionally-retry-logic-in-consumer-bean-deserialization-error")
        void sendNumber(int number)
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
    static interface ExpAndConditionalRetryErrorClient {
        @Topic("errors-exp-conditional-retry")
        void sendMessage(String message)
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaClient
    static interface RetryHandleAllErrorClient {
        @Topic("errors-retry-handle-all-exceptions")
        void sendMessage(String message)
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaClient
    static interface RetryReactiveHandleAllErrorClient {
        @Topic("errors-retry-reactive-handle-all-exceptions")
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

    @Requires(property = 'spec.name', value = 'KafkaErrorStrategySpec')
    @KafkaClient
    static interface RetryCountClient {
        void sendMessage(@Topic topic, String message)
    }
}

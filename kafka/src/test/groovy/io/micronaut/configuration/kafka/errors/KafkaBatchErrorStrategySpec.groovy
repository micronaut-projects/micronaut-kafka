package io.micronaut.configuration.kafka.errors

import io.micronaut.configuration.kafka.AbstractEmbeddedServerSpec
import io.micronaut.configuration.kafka.ConsumerRegistry
import io.micronaut.configuration.kafka.annotation.ErrorStrategy
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import io.micronaut.configuration.kafka.exceptions.KafkaListenerExceptionHandler
import io.micronaut.configuration.kafka.retry.ConditionalRetryBehaviourHandler
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import org.apache.kafka.common.errors.RecordDeserializationException

import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors

import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.*
import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST

class KafkaBatchErrorStrategySpec extends AbstractEmbeddedServerSpec {

    static final String BATCH_MODE_RESUME = "batch-mode-resume"
    static final String BATCH_MODE_RETRY = "batch-mode-retry"
    static final String BATCH_MODE_RETRY_CONDITIONALLY = "batch-mode-retry-conditionally"
    static final String BATCH_MODE_RETRY_EXP = "batch-mode-retry-exp"
    static final String BATCH_MODE_RETRY_CONDITIONALLY_EXP = "batch-mode-retry-conditionally-exp"
    static final String BATCH_MODE_RETRY_DESER = "batch-mode-retry-deser"
    static final String BATCH_MODE_RETRY_CONDITIONALLY_DESER = "batch-mode-retry-conditionally-deser"
    static final String BATCH_MODE_RETRY_HANDLE_ALL = "batch-mode-retry-handle-all"

    void "test batch mode with 'resume' error strategy"() {
        when: "A consumer throws an exception"
        MyClient myClient = context.getBean(MyClient)
        myClient.sendBatch(BATCH_MODE_RESUME, ['One', 'Two'])
        myClient.sendBatch(BATCH_MODE_RESUME, ['Three', 'Four'])

        ResumeConsumer myConsumer = context.getBean(ResumeConsumer)
        context.getBean(ConsumerRegistry).resume(BATCH_MODE_RESUME)

        then: "The batch that threw the exception was skipped and the next batch was processed"
        conditions.eventually {
            concatenate(myConsumer.received) == 'One/Two/Three/Four'
        }
    }

    void "test batch mode with 'retry' error strategy"() {
        when: "A consumer throws an exception"
        MyClient myClient = context.getBean(MyClient)
        myClient.sendBatch(BATCH_MODE_RETRY, ['One', 'Two'])
        myClient.sendBatch(BATCH_MODE_RETRY, ['Three', 'Four'])

        RetryConsumer myConsumer = context.getBean(RetryConsumer)
        context.getBean(ConsumerRegistry).resume(BATCH_MODE_RETRY)

        then: "The batch that threw the exception was re-consumed"
        conditions.eventually {
            myConsumer.received == ['One/Two', 'One/Two', 'Three/Four']
        }

        and: "The retry was delivered at least 50ms afterwards"
        myConsumer.times[1] - myConsumer.times[0] >= 500
    }

    void "test batch mode with 'retry conditionally' error strategy"() {
        when: "A consumer throws an exception"
        MyClient myClient = context.getBean(MyClient)
        myClient.sendBatch(BATCH_MODE_RETRY_CONDITIONALLY, ['One', 'Two'])
        myClient.sendBatch(BATCH_MODE_RETRY_CONDITIONALLY, ['Three', 'Four'])
        myClient.sendBatch(BATCH_MODE_RETRY_CONDITIONALLY, ['Five', 'Six'])

        RetryConditionallyConsumer myConsumer = context.getBean(RetryConditionallyConsumer)
        context.getBean(ConsumerRegistry).resume(BATCH_MODE_RETRY_CONDITIONALLY)

        then: "The batch that threw the exception was re-consumed except for the one we want to skip"
        conditions.eventually {
            myConsumer.received == ['One/Two', 'One/Two', 'Three/Four', 'Five/Six']
            myConsumer.successful == ['One/Two', 'Five/Six']
            myConsumer.skipped == ['Three/Four']
        }

        and: "The retry was delivered at least 50ms afterwards"
        myConsumer.times[1] - myConsumer.times[0] >= 500
    }

    void "test batch mode with 'retry' error strategy when there are serialization errors"() {
        when: "A record cannot be deserialized"
        MyClient myClient = context.getBean(MyClient)
        myClient.sendBatchOfNumbers(BATCH_MODE_RETRY_DESER, [111, 222])
        myClient.sendBatchOfNumbers(BATCH_MODE_RETRY_DESER, [333])
        myClient.sendBatch(BATCH_MODE_RETRY_DESER, ['Not an integer'])
        myClient.sendBatchOfNumbers(BATCH_MODE_RETRY_DESER, [444, 555])

        RetryDeserConsumer myConsumer = context.getBean(RetryDeserConsumer)
        context.getBean(ConsumerRegistry).resume(BATCH_MODE_RETRY_DESER)

        then: "The message that threw the exception was eventually left behind"
        conditions.eventually {
            myConsumer.received == ['111/222', '333', '444/555']
        }

        and: "The retry error strategy was honored"
        myConsumer.exceptions.size() == 2
        myConsumer.exceptions[0].message.startsWith('Error deserializing VALUE')
        (myConsumer.exceptions[0].cause as RecordDeserializationException).offset() == 3
        myConsumer.exceptions[1].message.startsWith('Error deserializing VALUE')
        (myConsumer.exceptions[1].cause as RecordDeserializationException).offset() == 3
    }

    void "test batch mode with 'retry conditionally' error strategy when there are serialization errors"() {
        when: "A record cannot be deserialized"
        MyClient myClient = context.getBean(MyClient)
        myClient.sendBatchOfNumbers(BATCH_MODE_RETRY_CONDITIONALLY_DESER, [111, 222])
        myClient.sendBatch(BATCH_MODE_RETRY_CONDITIONALLY_DESER, ['Not an integer and should be retried'])
        myClient.sendBatchOfNumbers(BATCH_MODE_RETRY_CONDITIONALLY_DESER, [333])
        myClient.sendBatch(BATCH_MODE_RETRY_CONDITIONALLY_DESER, ['Not an integer and should be skipped'])
        myClient.sendBatchOfNumbers(BATCH_MODE_RETRY_CONDITIONALLY_DESER, [444, 555])

        RetryConditionallyDeserConsumer myConsumer = context.getBean(RetryConditionallyDeserConsumer)
        context.getBean(ConsumerRegistry).resume(BATCH_MODE_RETRY_CONDITIONALLY_DESER)

        then: "The messages that threw the exception was eventually left behind and the one we wanted to skip was skipped"
        conditions.eventually {
            myConsumer.received == ['111/222', '333', '444/555']
            myConsumer.successful == ['111/222', '333', '444/555']
            myConsumer.skippedOffsets.contains(4L)
        }

        and: "The retry error strategy was honored"
        myConsumer.exceptions.size() == 2
        myConsumer.exceptions[0].message.startsWith('Error deserializing VALUE')
        (myConsumer.exceptions[0].cause as RecordDeserializationException).offset() == 2
        myConsumer.exceptions[1].message.startsWith('Error deserializing VALUE')
        (myConsumer.exceptions[1].cause as RecordDeserializationException).offset() == 4
    }

    void "test batch mode with 'retry exp' error strategy"() {
        when: "A consumer throws an exception"
        MyClient myClient = context.getBean(MyClient)
        myClient.sendBatch(BATCH_MODE_RETRY_EXP, ['One', 'Two'])
        myClient.sendBatch(BATCH_MODE_RETRY_EXP, ['Three', 'Four'])

        RetryExpConsumer myConsumer = context.getBean(RetryExpConsumer)
        context.getBean(ConsumerRegistry).resume(BATCH_MODE_RETRY_EXP)

        then: "Batch is consumed eventually"
        conditions.eventually {
            myConsumer.received == ['One/Two', 'One/Two', 'One/Two', 'One/Two', 'Three/Four']
        }

        and: "Batch was retried with exponential breaks between deliveries"
        myConsumer.times[1] - myConsumer.times[0] >= 50
        myConsumer.times[2] - myConsumer.times[1] >= 100
        myConsumer.times[3] - myConsumer.times[2] >= 200
    }

    void "test batch mode with 'retry conditionally exp' error strategy"() {
        when: "A consumer throws an exception"
        MyClient myClient = context.getBean(MyClient)
        myClient.sendBatch(BATCH_MODE_RETRY_CONDITIONALLY_EXP, ['One', 'Two'])
        myClient.sendBatch(BATCH_MODE_RETRY_CONDITIONALLY_EXP, ['Three', 'Four'])
        myClient.sendBatch(BATCH_MODE_RETRY_CONDITIONALLY_EXP, ['Five', 'Six'])

        RetryConditionallyExpConsumer myConsumer = context.getBean(RetryConditionallyExpConsumer)
        context.getBean(ConsumerRegistry).resume(BATCH_MODE_RETRY_CONDITIONALLY_EXP)

        then: "The first batch is retried until it is skipped, the second is skipped immediately, and the third batch is consumed eventually"
        conditions.eventually {
            myConsumer.received == ['One/Two', 'One/Two', 'One/Two', 'One/Two', 'Three/Four', 'Five/Six']
            myConsumer.successful == ['Five/Six']
            myConsumer.skipped == ['Three/Four']
        }

        and: "Batch was retried with exponential breaks between deliveries"
        myConsumer.times[1] - myConsumer.times[0] >= 50
        myConsumer.times[2] - myConsumer.times[1] >= 100
        myConsumer.times[3] - myConsumer.times[2] >= 200
    }

    void "test batch mode with 'retry' error strategy + handle all exceptions"() {
        when: "A consumer throws an exception"
        MyClient myClient = context.getBean(MyClient)
        myClient.sendBatch(BATCH_MODE_RETRY_HANDLE_ALL, ['One', 'Two'])
        myClient.sendBatch(BATCH_MODE_RETRY_HANDLE_ALL, ['Three', 'Four'])
        myClient.sendBatch(BATCH_MODE_RETRY_HANDLE_ALL, ['Five', 'Six'])
        myClient.sendBatch(BATCH_MODE_RETRY_HANDLE_ALL, ['Seven', 'Eight'])

        RetryHandleAllConsumer myConsumer = context.getBean(RetryHandleAllConsumer)
        context.getBean(ConsumerRegistry).resume(BATCH_MODE_RETRY_HANDLE_ALL)

        then: "Batches were retried and consumed eventually"
        conditions.eventually {
            myConsumer.received == ['One/Two', 'Three/Four', 'Three/Four', 'Five/Six', 'Five/Six', 'Five/Six', 'Seven/Eight']
        }

        and: "All exceptions were handled"
        myConsumer.exceptions.size() == 4
        myConsumer.exceptions[0].message == "[Three, Four] #2"
        myConsumer.exceptions[0].consumerRecords.orElseThrow()
        myConsumer.exceptions[1].message == "[Five, Six] #4"
        myConsumer.exceptions[1].consumerRecords.orElseThrow()
        myConsumer.exceptions[2].message == "[Five, Six] #5"
        myConsumer.exceptions[2].consumerRecords.orElseThrow()
        myConsumer.exceptions[3].message == "[Five, Six] #6"
        myConsumer.exceptions[3].consumerRecords.orElseThrow()
    }

    @Requires(property = 'spec.name', value = 'KafkaBatchErrorStrategySpec')
    @KafkaClient(batch = true)
    static interface MyClient {
        void sendBatch(@Topic String topic, List<String> messages)

        void sendBatchOfNumbers(@Topic String topic, List<Integer> numbers)
    }

    static abstract class AbstractConsumer {
        AtomicInteger count = new AtomicInteger(0)
        List<?> received = []
        List<KafkaListenerException> exceptions = []
        List<String> successful = []
        List<String> skipped = []
        List<Long> skippedOffsets = []
    }

    @Requires(property = 'spec.name', value = 'KafkaBatchErrorStrategySpec')
    @KafkaListener(
            clientId = BATCH_MODE_RESUME,
            batch = true,
            autoStartup = false,
            offsetReset = EARLIEST,
            errorStrategy = @ErrorStrategy(value = RESUME_AT_NEXT_RECORD),
            properties = @Property(name = 'max.poll.records', value = '2'))
    static class ResumeConsumer extends AbstractConsumer {
        @Topic(BATCH_MODE_RESUME)
        void receiveBatch(List<String> messages) {
            received << concatenate(messages)
            if (count.getAndIncrement() == 0) throw new RuntimeException("Won't handle first batch")
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaBatchErrorStrategySpec')
    @KafkaListener(
            clientId = BATCH_MODE_RETRY,
            batch = true,
            autoStartup = false,
            offsetReset = EARLIEST,
            errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR, retryDelay = '500ms'),
            properties = @Property(name = 'max.poll.records', value = '2'))
    static class RetryConsumer extends AbstractConsumer {
        List<Long> times = []

        @Topic(BATCH_MODE_RETRY)
        void receiveBatch(List<String> messages) {
            received << concatenate(messages)
            times << System.currentTimeMillis()
            if (count.getAndIncrement() == 0) {
                throw new RuntimeException("Won't handle first batch")
            }
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaBatchErrorStrategySpec')
    @KafkaListener(
            clientId = BATCH_MODE_RETRY_CONDITIONALLY,
            batch = true,
            autoStartup = false,
            offsetReset = EARLIEST,
            errorStrategy = @ErrorStrategy(value = RETRY_CONDITIONALLY_ON_ERROR, retryDelay = '500ms'),
            properties = @Property(name = 'max.poll.records', value = '2'))
    static class RetryConditionallyConsumer extends AbstractConsumer implements ConditionalRetryBehaviourHandler {
        List<Long> times = []

        @Topic(BATCH_MODE_RETRY_CONDITIONALLY)
        void receiveBatch(List<String> messages) {
            received << concatenate(messages)
            times << System.currentTimeMillis()
            def current = count.getAndIncrement()
            if (current == 0 || current == 2) {
                throw new RuntimeException("Won't handle first attempt of first and third batches")
            }
            successful << concatenate(messages)
        }

        @Override
        ConditionalRetryBehaviour conditionalRetryBehaviour(KafkaListenerException exception) {
            def message = exception.getConsumerRecords().get().asList().stream()
                    .map(it -> it.value())
                    .collect(Collectors.joining("/"))
            if (message == "Three/Four") {
                skipped << message
                return ConditionalRetryBehaviour.SKIP
            } else {
                return ConditionalRetryBehaviour.RETRY
            }
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaBatchErrorStrategySpec')
    @KafkaListener(
            clientId = BATCH_MODE_RETRY_DESER,
            batch = true,
            autoStartup = false,
            offsetReset = EARLIEST,
            errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR, handleAllExceptions = true),
            properties = @Property(name = 'max.poll.records', value = '2'))
    static class RetryDeserConsumer extends AbstractConsumer implements KafkaListenerExceptionHandler {

        @Topic(BATCH_MODE_RETRY_DESER)
        void receiveBatch(List<Integer> numbers) {
            received << concatenate(numbers)
        }

        @Override
        void handle(KafkaListenerException exception) {
            exceptions << exception
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaBatchErrorStrategySpec')
    @KafkaListener(
            clientId = BATCH_MODE_RETRY_CONDITIONALLY_DESER,
            batch = true,
            autoStartup = false,
            offsetReset = EARLIEST,
            errorStrategy = @ErrorStrategy(value = RETRY_CONDITIONALLY_ON_ERROR, handleAllExceptions = true),
            properties = @Property(name = 'max.poll.records', value = '2'))
    static class RetryConditionallyDeserConsumer extends AbstractConsumer implements KafkaListenerExceptionHandler, ConditionalRetryBehaviourHandler {

        @Topic(BATCH_MODE_RETRY_CONDITIONALLY_DESER)
        void receiveBatch(List<Integer> numbers) {
            received << concatenate(numbers)
            successful << concatenate(numbers)
        }

        @Override
        void handle(KafkaListenerException exception) {
            exceptions << exception
        }

        @Override
        ConditionalRetryBehaviour conditionalRetryBehaviour(KafkaListenerException exception) {
            if(exception.message.concat("at offset 4")) {
                skippedOffsets << 4L
                return ConditionalRetryBehaviour.SKIP
            } else {
                return ConditionalRetryBehaviour.RETRY
            }
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaBatchErrorStrategySpec')
    @KafkaListener(
            clientId = BATCH_MODE_RETRY_EXP,
            batch = true,
            autoStartup = false,
            offsetReset = EARLIEST,
            errorStrategy = @ErrorStrategy(value = RETRY_EXPONENTIALLY_ON_ERROR, retryCount = 3, retryDelay = '50ms'),
            properties = @Property(name = 'max.poll.records', value = '2'))
    static class RetryExpConsumer extends AbstractConsumer {
        List<Long> times = []

        @Topic(BATCH_MODE_RETRY_EXP)
        void receiveBatch(List<String> messages) {
            received << concatenate(messages)
            times << System.currentTimeMillis()
            if (count.getAndIncrement() < 4) {
                throw new RuntimeException("Won't handle first three delivery attempts")
            }
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaBatchErrorStrategySpec')
    @KafkaListener(
            clientId = BATCH_MODE_RETRY_CONDITIONALLY_EXP,
            batch = true,
            autoStartup = false,
            offsetReset = EARLIEST,
            errorStrategy = @ErrorStrategy(value = RETRY_CONDITIONALLY_EXPONENTIALLY_ON_ERROR, retryCount = 3, retryDelay = '50ms'),
            properties = @Property(name = 'max.poll.records', value = '2'))
    static class RetryConditionallyExpConsumer extends AbstractConsumer implements ConditionalRetryBehaviourHandler {
        List<Long> times = []

        @Topic(BATCH_MODE_RETRY_CONDITIONALLY_EXP)
        void receiveBatch(List<String> messages) {
            received << concatenate(messages)
            times << System.currentTimeMillis()
            def current = count.getAndIncrement()
            if (current < 5) {
                throw new RuntimeException("Won't handle first batch and fail on the first attempt of the second batch")
            }
            successful << concatenate(messages)
        }

        @Override
        ConditionalRetryBehaviour conditionalRetryBehaviour(KafkaListenerException exception) {
            def message = exception.getConsumerRecords().get().asList().stream()
                    .map(it -> it.value())
                    .collect(Collectors.joining("/"))
            if (message == "Three/Four") {
                skipped << message
                return ConditionalRetryBehaviour.SKIP
            } else {
                return ConditionalRetryBehaviour.RETRY
            }
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaBatchErrorStrategySpec')
    @KafkaListener(
            clientId = BATCH_MODE_RETRY_HANDLE_ALL,
            batch = true,
            autoStartup = false,
            offsetReset = EARLIEST,
            errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR, retryCount = 2, handleAllExceptions = true),
            properties = @Property(name = "max.poll.records", value = "2"))
    static class RetryHandleAllConsumer extends AbstractConsumer implements KafkaListenerExceptionHandler {

        @Topic(BATCH_MODE_RETRY_HANDLE_ALL)
        void receiveBatch(List<String> messages) {
            received << concatenate(messages)
            if (count.getAndIncrement() == 1 || messages.contains('Five')) throw new RuntimeException("${messages} #${count}")
        }

        @Override
        void handle(KafkaListenerException exception) {
            exceptions << exception
        }
    }

    static String concatenate(List<?> messages) {
        return messages.stream().map(Object::toString).collect(Collectors.joining('/'))
    }
}

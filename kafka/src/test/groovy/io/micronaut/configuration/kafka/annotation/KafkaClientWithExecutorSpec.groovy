package io.micronaut.configuration.kafka.annotation

import io.micronaut.configuration.kafka.AbstractKafkaSpec
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.core.io.socket.SocketUtils
import io.micronaut.scheduling.TaskExecutors
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import spock.lang.AutoCleanup
import spock.util.concurrent.PollingConditions

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

import static io.micronaut.configuration.kafka.annotation.KafkaClient.Acknowledge.ALL
import static io.micronaut.core.io.socket.SocketUtils.LOCALHOST

class KafkaClientWithExecutorSpec extends AbstractKafkaSpec {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaClientWithExecutorSpec.class)

    @AutoCleanup
    private ApplicationContext ctx

    void "test reactive send message with executor set via default config props does not block the calling thread when Kafka is not available"() {
        given: "the default executor is specified"
        ctx = ApplicationContext.run(
                getConfiguration() +
                        ['kafka.bootstrap.servers': LOCALHOST + ':' + SocketUtils.findAvailableTcpPort(),
                         'kafka.producers.default.executor': TaskExecutors.BLOCKING])
        MyDefaultExecutorClient client = ctx.getBean(MyDefaultExecutorClient)

        when: "client operations are invoked while Kafka is unavailable"
        AtomicInteger failureCount = new AtomicInteger(0)
        List<Mono<String>> sendOps = new ArrayList<>()
        for (int x=0; x<3; x++) {
            sendOps.add(client.sendRx("test", "hello-world", new RecordHeaders([new RecordHeader("hello", "world".bytes)])))
        }
        sendOps.stream().map(sendOp -> {
            sendOp.subscribe(s -> { throw new IllegalStateException("Unexpected result") },
                    ex -> {
                        int currentCount = failureCount.incrementAndGet()
                        LOG.debug("Subscriber failure # {} : {}", currentCount, ex.getMessage())
                    })
        }).toList()

        then: "the operations should not block the calling thread while waiting for the maxBlock timeout"
        failureCount.get() == 0
    }

    void "test reactive send message with executor set via named config props does not block the calling thread when Kafka is not available"() {
        given: "the executor is specified via named producer config"
        ctx = ApplicationContext.run(
                getConfiguration() +
                        ['kafka.bootstrap.servers': LOCALHOST + ':' + SocketUtils.findAvailableTcpPort(),
                         'kafka.producers.scorsese.executor': TaskExecutors.IO])
        MyNamedExecutorClient client = ctx.getBean(MyNamedExecutorClient)

        when: "client operations are invoked while Kafka is unavailable"
        AtomicInteger failureCount = new AtomicInteger(0)
        List<Mono<String>> sendOps = new ArrayList<>()
        for (int x=0; x<3; x++) {
            sendOps.add(client.sendRx("test", "hello-world", new RecordHeaders([new RecordHeader("hello", "world".bytes)])))
        }
        sendOps.stream().map(sendOp -> {
            sendOp.subscribe(s -> { throw new IllegalStateException("Unexpected result") },
                    ex -> {
                        int currentCount = failureCount.incrementAndGet()
                        LOG.debug("Subscriber failure # {} : {}", currentCount, ex.getMessage())
                    })
        }).toList()

        then: "the operations should not block the calling thread while waiting for the maxBlock timeout"
        failureCount.get() == 0
    }

    void "test reactive send message with executor set via annotation does not block the calling thread when Kafka is not available"() {
        given: "the executor is specified via annotation"
        ctx = ApplicationContext.run(
                getConfiguration() +
                        ['kafka.bootstrap.servers': LOCALHOST + ':' + SocketUtils.findAvailableTcpPort()])
        MyAnnotatedExecutorClient client = ctx.getBean(MyAnnotatedExecutorClient)
        PollingConditions conditions = new PollingConditions()
        conditions.initialDelay = 1

        when: "client operations are invoked while Kafka is unavailable"
        AtomicInteger failureCount = new AtomicInteger(0)
        List<Mono<String>> sendOps = new ArrayList<>()
        for (int x=0; x<3; x++) {
            sendOps.add(client.sendRx("test", "hello-world", new RecordHeaders([new RecordHeader("hello", "world".bytes)])))
        }
        sendOps.stream().map(sendOp -> {
            sendOp.subscribe(s -> { throw new IllegalStateException("Unexpected result") },
                    ex -> {
                        int currentCount = failureCount.incrementAndGet()
                        LOG.debug("Subscriber failure # {} : {}", currentCount, ex.getMessage())
                    })
        }).toList()

        then: "the operations should not block the calling thread while waiting for the maxBlock timeout"
        failureCount.get() == 0
    }

    void "test future send message with executor set via default config props does not block the calling thread when Kafka is not available"() {
        given: "the default executor is specified"
        ctx = ApplicationContext.run(
                getConfiguration() +
                        ['kafka.bootstrap.servers': LOCALHOST + ':' + SocketUtils.findAvailableTcpPort(),
                         'kafka.producers.default.executor': TaskExecutors.BLOCKING])
        MyDefaultExecutorClient client = ctx.getBean(MyDefaultExecutorClient)

        when: "client operations are invoked while Kafka is unavailable"
        AtomicInteger failureCount = new AtomicInteger(0)
        List<CompletableFuture<String>> sendOps = new ArrayList<>()
        for (int x=0; x<3; x++) {
            sendOps.add(client.sendSentence("test", "hello-world"))
        }
        sendOps.stream().map(sendOp -> {
            sendOp.exceptionally {
                int currentCount = failureCount.incrementAndGet()
                LOG.debug("Subscriber failure # {} : {}", currentCount, it.getMessage())
            }}).toList()

        then: "the operations should not block the calling thread while waiting for the maxBlock timeout"
        failureCount.get() == 0
    }

    void "test future send message with executor set via named config props does not block the calling thread when Kafka is not available"() {
        given: "the executor is specified via named producer config"
        ctx = ApplicationContext.run(
                getConfiguration() +
                        ['kafka.bootstrap.servers': LOCALHOST + ':' + SocketUtils.findAvailableTcpPort(),
                         'kafka.producers.scorsese.executor': TaskExecutors.IO])
        MyNamedExecutorClient client = ctx.getBean(MyNamedExecutorClient)

        when: "client operations are invoked while Kafka is unavailable"
        AtomicInteger failureCount = new AtomicInteger(0)
        List<CompletableFuture<String>> sendOps = new ArrayList<>()
        for (int x=0; x<3; x++) {
            sendOps.add(client.sendSentence("test", "hello-world"))
        }
        sendOps.stream().map(sendOp -> {
            sendOp.exceptionally {
                int currentCount = failureCount.incrementAndGet()
                LOG.debug("Subscriber failure # {} : {}", currentCount, it.getMessage())
            }}).toList()

        then: "the operations should not block the calling thread while waiting for the maxBlock timeout"
        failureCount.get() == 0
    }

    void "test future send message with executor set via annotation does not block the calling thread when Kafka is not available"() {
        given: "the executor is specified via annotation"
        ctx = ApplicationContext.run(
                getConfiguration() +
                        ['kafka.bootstrap.servers': LOCALHOST + ':' + SocketUtils.findAvailableTcpPort()])
        MyAnnotatedExecutorClient client = ctx.getBean(MyAnnotatedExecutorClient)

        when: "client operations are invoked while Kafka is unavailable"
        AtomicInteger failureCount = new AtomicInteger(0)
        List<CompletableFuture<String>> sendOps = new ArrayList<>()
        for (int x=0; x<3; x++) {
            sendOps.add(client.sendSentence("test", "hello-world"))
        }
        sendOps.stream().map(sendOp -> {
            sendOp.exceptionally {
                int currentCount = failureCount.incrementAndGet()
                LOG.debug("Subscriber failure # {} : {}", currentCount, it.getMessage())
            }}).toList()

        then: "the operations should not block the calling thread while waiting for the maxBlock timeout"
        failureCount.get() == 0
    }

    @Requires(property = 'spec.name', value = 'KafkaClientWithExecutorSpec')
    @KafkaClient(maxBlock = '2s', acks = ALL)
    static interface MyDefaultExecutorClient {

        @Topic("words")
        CompletableFuture<String> sendSentence(@KafkaKey String key, String sentence)

        @Topic("words")
        Mono<String> sendRx(@KafkaKey String key, String sentence, Headers headers)
    }

    @Requires(property = 'spec.name', value = 'KafkaClientWithExecutorSpec')
    @KafkaClient(id = 'scorsese', maxBlock = '2s', acks = ALL)
    static interface MyNamedExecutorClient {

        @Topic("words")
        CompletableFuture<String> sendSentence(@KafkaKey String key, String sentence)

        @Topic("words")
        Mono<String> sendRx(@KafkaKey String key, String sentence, Headers headers)
    }

    @Requires(property = 'spec.name', value = 'KafkaClientWithExecutorSpec')
    @KafkaClient(maxBlock = '2s', acks = ALL, executor = TaskExecutors.BLOCKING)
    static interface MyAnnotatedExecutorClient {

        @Topic("words")
        CompletableFuture<String> sendSentence(@KafkaKey String key, String sentence)

        @Topic("words")
        Mono<String> sendRx(@KafkaKey String key, String sentence, Headers headers)
    }
}

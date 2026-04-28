package io.micronaut.configuration.kafka.offsets

import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.configuration.kafka.annotation.ErrorStrategy
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.serde.annotation.Serdeable
import jakarta.inject.Singleton

import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RESUME_AT_NEXT_RECORD
import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.SYNC_PER_RECORD
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

class PerRecordOffsetCommitSpec extends AbstractKafkaContainerSpec {

    public static final String TOPIC_SYNC = "PerRecordOffsetCommitSpec-products-sync"
    public static final String TOPIC_SYNC_RESUME = "PerRecordOffsetCommitSpec-products-sync-resume"
    public static final String SYNC_RESUME_GROUP = "per-record-resume-group"

    protected Map<String, Object> getConfiguration() {
        super.configuration +
                [(EMBEDDED_TOPICS): [TOPIC_SYNC, TOPIC_SYNC_RESUME]]
    }

    @Override
    void afterKafkaStarted() {
        createTopic(TOPIC_SYNC, 1, 1)
        createTopic(TOPIC_SYNC_RESUME, 1, 1)
    }

    void "test sync per record"() {
        given:
        ProductClient client = context.getBean(ProductClient)
        ProductListener listener = context.getBean(ProductListener)

        when:
        client.send(new Product(name: "Apple"))
        client.send(new Product(name: "Orange"))

        then:
        conditions.eventually {
            listener.products.size() == 2
            listener.products.find() { it.name == "Apple"}
        }
    }

    void "test sync per record resume commits the failed offset across restart"() {
        given:
        ResumeProductClient client = context.getBean(ResumeProductClient)
        ResumeProductListener listener = context.getBean(ResumeProductListener)

        when:
        client.send(new Product(name: "Apple"))
        client.send(new Product(name: "Boom"))

        then:
        conditions.eventually {
            listener.processedNames == ["Apple"]
            listener.attemptedNames == ["Apple", "Boom"]
            listener.failures.get() == 1
        }

        when:
        context.close()
        startContext()
        client = context.getBean(ResumeProductClient)
        listener = context.getBean(ResumeProductListener)
        client.send(new Product(name: "Pear"))

        then:
        conditions.eventually {
            listener.processedNames == ["Pear"]
            listener.attemptedNames == ["Pear"]
            listener.failures.get() == 0
        }
    }

    @Requires(property = 'spec.name', value = 'PerRecordOffsetCommitSpec')
    @KafkaClient
    static interface ProductClient {
        @Topic(PerRecordOffsetCommitSpec.TOPIC_SYNC)
        void send(Product product)
    }

    @Requires(property = 'spec.name', value = 'PerRecordOffsetCommitSpec')
    @Singleton
    static class ProductListener {

        List<Product> products = []

        @KafkaListener(offsetReset = EARLIEST, offsetStrategy = SYNC_PER_RECORD)
        @Topic(PerRecordOffsetCommitSpec.TOPIC_SYNC)
        void receive(Product product) {
            products << product
        }
    }

    @Requires(property = 'spec.name', value = 'PerRecordOffsetCommitSpec')
    @KafkaClient
    static interface ResumeProductClient {
        @Topic(PerRecordOffsetCommitSpec.TOPIC_SYNC_RESUME)
        void send(Product product)
    }

    @Requires(property = 'spec.name', value = 'PerRecordOffsetCommitSpec')
    @Singleton
    static class ResumeProductListener {

        List<String> attemptedNames = new CopyOnWriteArrayList<>()
        List<String> processedNames = new CopyOnWriteArrayList<>()
        AtomicInteger failures = new AtomicInteger()

        @KafkaListener(
            groupId = SYNC_RESUME_GROUP,
            offsetReset = EARLIEST,
            offsetStrategy = SYNC_PER_RECORD,
            errorStrategy = @ErrorStrategy(value = RESUME_AT_NEXT_RECORD)
        )
        @Topic(PerRecordOffsetCommitSpec.TOPIC_SYNC_RESUME)
        void receive(Product product) {
            attemptedNames << product.name
            if (product.name == "Boom") {
                failures.incrementAndGet()
                throw new IllegalStateException("boom")
            }
            processedNames << product.name
        }
    }

    @Serdeable
    static class Product {
        String name
    }
}

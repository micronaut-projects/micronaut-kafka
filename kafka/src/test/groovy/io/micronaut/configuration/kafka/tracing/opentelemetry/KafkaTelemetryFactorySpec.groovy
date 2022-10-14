package io.micronaut.configuration.kafka.tracing.opentelemetry

import io.micronaut.configuration.kafka.AbstractKafkaSpec
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.opentelemetry.api.common.AttributeType
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.internal.InternalAttributeKeyImpl
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.header.internals.RecordHeaders

class KafkaTelemetryFactorySpec extends AbstractKafkaSpec {

    void "test kafka telemetry headers config"() {
        given:
        ApplicationContext ctx = ApplicationContext.run(
                getConfiguration() + [
                        "kafka.otel.enabled"         : "true",
                        "kafka.otel.captured-headers": [
                                "test",
                                "myHeader"
                        ]
                ])

        when:
        def kafkaTelemetryFactory = ctx.getBean(KafkaTelemetryFactory)
        def kafkaTelemetryConfig = ctx.getBean(KafkaTelemetryConfiguration)

        then:
        kafkaTelemetryFactory
        kafkaTelemetryConfig

        when:
        def attributesBuilder = Attributes.builder()
        def headers = new RecordHeaders()
        headers.add("test", "myTest".bytes)
        headers.add("myHeader", "myValue".bytes)
        headers.add("myHeader2", "myValue2".bytes)
        kafkaTelemetryFactory.putAttributes(attributesBuilder, headers, kafkaTelemetryConfig)

        then:
        def attrs = attributesBuilder.build()
        attrs.get(InternalAttributeKeyImpl.create("messaging.header.test", AttributeType.STRING)) == "myTest"
        attrs.get(InternalAttributeKeyImpl.create("messaging.header.myHeader", AttributeType.STRING)) == "myValue"
        !attrs.get(InternalAttributeKeyImpl.create("messaging.header.myHeader2", AttributeType.STRING))

        cleanup:
        ctx.close()
    }

    void "test kafka telemetry all headers config"() {
        given:
        ApplicationContext ctx = ApplicationContext.run(
                getConfiguration() + [
                        "kafka.otel.enabled": "true",
                ])

        when:
        def kafkaTelemetryFactory = ctx.getBean(KafkaTelemetryFactory)
        def kafkaTelemetryConfig = ctx.getBean(KafkaTelemetryConfiguration)

        then:
        kafkaTelemetryFactory
        kafkaTelemetryConfig

        when:
        def attributesBuilder = Attributes.builder()
        def headers = new RecordHeaders()
        headers.add("test", "myTest".bytes)
        headers.add("myHeader", "myValue".bytes)
        headers.add("myHeader2", "myValue2".bytes)
        kafkaTelemetryFactory.putAttributes(attributesBuilder, headers, kafkaTelemetryConfig)

        then:
        def attrs = attributesBuilder.build()
        attrs.get(InternalAttributeKeyImpl.create("messaging.header.test", AttributeType.STRING)) == "myTest"
        attrs.get(InternalAttributeKeyImpl.create("messaging.header.myHeader", AttributeType.STRING)) == "myValue"
        attrs.get(InternalAttributeKeyImpl.create("messaging.header.myHeader2", AttributeType.STRING)) == "myValue2"

        cleanup:
        ctx.close()
    }

    void "test kafka telemetry none headers config"() {
        given:
        ApplicationContext ctx = ApplicationContext.run(
                getConfiguration() + [
                        "kafka.otel.enabled": "true",
                        "kafka.otel.captured-headers": [
                                ''
                        ]
                ])

        when:
        def kafkaTelemetryFactory = ctx.getBean(KafkaTelemetryFactory)
        def kafkaTelemetryConfig = ctx.getBean(KafkaTelemetryConfiguration)

        then:
        kafkaTelemetryFactory
        kafkaTelemetryConfig

        when:
        def attributesBuilder = Attributes.builder()
        def headers = new RecordHeaders()
        headers.add("test", "myTest".bytes)
        headers.add("myHeader", "myValue".bytes)
        headers.add("myHeader2", "myValue2".bytes)
        kafkaTelemetryFactory.putAttributes(attributesBuilder, headers, kafkaTelemetryConfig)

        then:
        def attrs = attributesBuilder.build()
        !attrs.get(InternalAttributeKeyImpl.create("messaging.header.test", AttributeType.STRING))
        !attrs.get(InternalAttributeKeyImpl.create("messaging.header.myHeader", AttributeType.STRING))
        !attrs.get(InternalAttributeKeyImpl.create("messaging.header.myHeader2", AttributeType.STRING))

        cleanup:
        ctx.close()
    }

    @Requires(property = 'spec.name', value = 'KafkaClientScopeSpec')
    @Singleton
    static class MyClass {
        @Inject
        @KafkaClient("foo")
        Producer<String, Integer> producer
    }
}


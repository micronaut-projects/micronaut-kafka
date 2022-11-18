package io.micronaut.configuration.kafka.scope

import io.micronaut.configuration.kafka.AbstractKafkaSpec
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.apache.kafka.clients.producer.Producer

class KafkaClientScopeSpec extends AbstractKafkaSpec {

    void "test inject kafka producer"() {
        given:
        ApplicationContext ctx = ApplicationContext.run(
                getConfiguration() +
                ["kafka.producers.foo.acks": "all"])

        when:
        MyClass myClass = ctx.getBean(MyClass)

        then:
        myClass.producer != null
        // 'all' gets translated to '-1' by kafka
        myClass.producer.@producerConfig.getString("acks") == "-1"

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

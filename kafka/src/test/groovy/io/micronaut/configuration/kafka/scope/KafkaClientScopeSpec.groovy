package io.micronaut.configuration.kafka.scope

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.context.ApplicationContext
import org.apache.kafka.clients.producer.Producer
import spock.lang.Specification

import javax.inject.Inject
import javax.inject.Singleton

class KafkaClientScopeSpec extends Specification {

    void "test inject kafka producer"() {
        given:
        ApplicationContext ctx = ApplicationContext.run(
                "kafka.producers.foo.acks":"all"
        )

        when:
        MyClass myClass = ctx.getBean(MyClass)

        then:
        myClass.producer != null
        // 'all' gets translated to '-1' by kafka
        myClass.producer.@producer.@producerConfig.getString("acks") == "-1"

        cleanup:
        ctx.close()
    }

    @Singleton
    static class MyClass {
        @Inject
        @KafkaClient("foo")
        Producer<String, Integer> producer
    }
}

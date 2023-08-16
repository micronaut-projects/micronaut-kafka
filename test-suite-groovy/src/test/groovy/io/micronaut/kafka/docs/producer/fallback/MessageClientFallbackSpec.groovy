package io.micronaut.kafka.docs.producer.fallback

import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Specification

@MicronautTest
@Property(name = "spec.name", value = "MessageClientFallbackSpec")
@Property(name = "kafka.enabled", value = "false")
class MessageClientFallbackSpec extends Specification {

    @Inject
    ApplicationContext context

    void "test that context contains the fallback bean"() {
        when:
        MessageClientFallback bean = context.getBean(MessageClientFallback)

        then:
        bean != null

        when:
        bean.send('message')

        then:
        thrown(UnsupportedOperationException)
    }
}

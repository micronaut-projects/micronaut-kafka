package io.micronaut.test

import io.micronaut.context.ApplicationContext
import jakarta.inject.Singleton
import spock.lang.Specification

/**
 * Test that micronaut-kafka can be disabled by setting 'kafka.enabled': 'false'.
 *
 * Note that this test and associated classes are in a different package so that they are not affected by the package-level
 * annotation which would otherwise disable them.
 */
class DisabledSpec extends Specification {

    void "Starting app with kafka disabled works correctly"() {
        given:
        ApplicationContext ctx = ApplicationContext.run("kafka.enabled": "false")

        when: "test that the service has been created correctly"
        DisabledTestService service = ctx.getBean(DisabledTestService)

        then:
        service

        when: "test that the fallback client has been injected"
        service.disabledClient instanceof DisabledClientFallback
        service.send()

        then:
        noExceptionThrown()

        and: "test that the consumer bean is still created correctly"
        service.getNum() == 1

        cleanup:
        ctx.close()
    }

    @Singleton
    static class DisabledTestService {
        private final DisabledClient disabledClient
        private final DisabledConsumer disabledConsumer

        DisabledTestService(DisabledClient disabledClient, DisabledConsumer disabledConsumer) {
            this.disabledClient = disabledClient
            this.disabledConsumer = disabledConsumer
        }

        void send() {
            disabledClient.send "Hello, World!"
        }

        int getNum() {
            disabledConsumer.num
        }
    }
}

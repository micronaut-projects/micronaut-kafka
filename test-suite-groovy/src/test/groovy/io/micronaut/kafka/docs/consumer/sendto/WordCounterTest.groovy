package io.micronaut.kafka.docs.consumer.sendto

import io.micronaut.context.ApplicationContext
import spock.lang.Specification

class WordCounterTest extends Specification {

    void "test Word Counter"() {
        given:
        ApplicationContext ctx = ApplicationContext.run(
                'kafka.enabled': true, 'spec.name': 'WordCounterTest'
        )

        when:
        WordCounterClient client = ctx.getBean(WordCounterClient.class)
        client.send('Test to test for words')

        then:
        noExceptionThrown()
    }
}

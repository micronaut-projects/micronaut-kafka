package io.micronaut.kafka.docs.consumer.sendto

import io.micronaut.context.ApplicationContext
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import static java.util.concurrent.TimeUnit.SECONDS

class WordCounterTest extends Specification {

    void "test Word Counter"() {
        given:
        ApplicationContext ctx = ApplicationContext.run(
                'kafka.enabled': true, 'spec.name': 'WordCounterTest'
        )

        when:
        WordCounterClient client = ctx.getBean(WordCounterClient.class)
        client.send('test to test for words')

        then:
        WordCountListener listener = ctx.getBean(WordCountListener.class)
        new PollingConditions(timeout: 10).eventually {
            listener.wordCount.size()       == 4
            listener.wordCount.get("test")  == 2
            listener.wordCount.get("to")    == 1
            listener.wordCount.get("for")   == 1
            listener.wordCount.get("words") == 1
        }
    }
}

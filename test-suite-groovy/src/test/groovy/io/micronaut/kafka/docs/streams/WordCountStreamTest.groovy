package io.micronaut.kafka.docs.streams

import io.micronaut.context.ApplicationContext
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

class WordCountStreamTest extends Specification {

    PollingConditions conditions = new PollingConditions()

    void "test word counter"() {
        given:
        ApplicationContext ctx = ApplicationContext.run(
                'kafka.enabled': true, 'spec.name': 'WordCountStreamTest'
        )

        when:
        WordCountClient client = ctx.getBean(WordCountClient)
        client.publishSentence('test to test for words')

        then:
        WordCountListener listener = ctx.getBean(WordCountListener)
        conditions.within(10) {
            listener.getWordCounts().size() == 4 &&
            listener.getCount('test')  == 2 &&
            listener.getCount('to')    == 1 &&
            listener.getCount('for')   == 1 &&
            listener.getCount('words') == 1
        }

        cleanup:
        ctx.close()
    }
}

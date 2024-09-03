package io.micronaut.configuration.kafka.scope

import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.serde.annotation.Serdeable
import jakarta.inject.Inject
import jakarta.inject.Singleton
import spock.lang.Retry

import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

@Retry
class PollScopeSpec extends AbstractKafkaContainerSpec {

  public static final String BOOKS_TOPIC = 'PollScopeSpec-books'

  protected Map<String, Object> getConfiguration() {
    super.configuration +
      [(EMBEDDED_TOPICS): [BOOKS_TOPIC]]
  }

  void "PollScope beans are injected per kafka consumer and updated before processing records"() {
    given:
    MyClient myClient = context.getBean(MyClient)
    MyListener myListener = context.getBean(MyListener)
    MyListener2 myListener2 = context.getBean(MyListener2)

    when:
    (0..10).forEach {
      myClient.sendBook(new Book(title: 'title'))
    }

    then:
    conditions.eventually {
      myListener.seenNumbers.size() == 20
      myListener2.seenNumbers.size() == 20
    }
    (myListener.seenNumbers.unique() + myListener2.seenNumbers.unique()).size() == 20
  }

  @Requires(property = 'spec.name', value = 'PollScopeSpec')
  @PollScope
  static class PollClass{
    Double random = Math.random()
  }

  @Requires(property = 'spec.name', value = 'PollScopeSpec')
  @KafkaListener(batch = false, offsetReset = OffsetReset.EARLIEST)
  static class MyListener {

    @Inject
    PollClass pollClass

    @Inject
    MyService myService

    List<Double> seenNumbers = []

    @Topic(PollScopeSpec.BOOKS_TOPIC)
    void receiveBook(Book book) {
      seenNumbers.add(pollClass.random)
      seenNumbers.add(myService.getPollClassValue())
    }
  }

  @Requires(property = 'spec.name', value = 'PollScopeSpec')
  @KafkaListener(batch = false, offsetReset = OffsetReset.EARLIEST)
  static class MyListener2 {

    @Inject
    PollClass pollClass

    @Inject
    MyService myService

    List<Double> seenNumbers = []

    @Topic(PollScopeSpec.BOOKS_TOPIC)
    void receiveBook(Book book) {
      seenNumbers.add(pollClass.random)
      seenNumbers.add(myService.getPollClassValue())
    }
  }

  @Requires(property = 'spec.name', value = 'PollScopeSpec')
  @Singleton
  static class MyService {
    @Inject
    PollClass pollClass

    Double getPollClassValue(){
      return pollClass.random
    }
  }

  @Requires(property = 'spec.name', value = 'PollScopeSpec')
  @KafkaClient
  static interface MyClient {
    @Topic(PollScopeSpec.BOOKS_TOPIC)
    void sendBook(Book book)
  }

  @ToString(includePackage = false)
  @EqualsAndHashCode
  @Serdeable
  static class Book {
    String title
  }
}

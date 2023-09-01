package io.micronaut.configuration.kafka.annotation

import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.context.annotation.Requires
import io.micronaut.messaging.annotation.MessageBody
import io.micronaut.messaging.annotation.MessageHeader
import io.micronaut.messaging.annotation.SendTo
import io.micronaut.serde.annotation.Serdeable
import org.apache.kafka.clients.consumer.ConsumerRecord
import reactor.core.publisher.Flux
import spock.lang.Retry

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

@Retry
class KafkaBatchListenerSpec extends AbstractKafkaContainerSpec {

    public static final String BOOKS_TOPIC = 'KafkaBatchListenerSpec-books'
    public static final String BOOKS_LIST_TOPIC = 'KafkaBatchListenerSpec-books-list'
    public static final String BOOK_CONSUMER_RECORD_LIST_TOPIC = 'KafkaBatchListenerSpec-consumer-records'
    public static final String BOOKS_HEADERS_TOPIC = 'KafkaBatchListenerSpec-books-headers'
    public static final String BOOKS_FLUX_TOPIC = 'KafkaBatchListenerSpec-books-flux'
    public static final String BOOKS_FORWARD_LIST_TOPIC = 'KafkaBatchListenerSpec-books-forward-list'
    public static final String BOOKS_FORWARD_ARRAY_TOPIC = 'KafkaBatchListenerSpec-books-forward-array'
    public static final String BOOKS_FORWARD_FLUX_TOPIC = 'KafkaBatchListenerSpec-books-forward-flux'
    public static final String BOOKS_ARRAY_TOPIC = 'KafkaBatchListenerSpec-books-array'
    public static final String TITLES_TOPIC = 'KafkaBatchListenerSpec-titles'

    protected Map<String, Object> getConfiguration() {
        super.configuration +
                [(EMBEDDED_TOPICS): [TITLES_TOPIC,
                                     BOOKS_LIST_TOPIC,
                                     BOOKS_ARRAY_TOPIC,
                                     BOOKS_TOPIC,
                                     BOOKS_FORWARD_LIST_TOPIC]
                ]
    }

    void "test send batch list with headers - blocking"() {
        given:
        MyBatchClient myBatchClient = context.getBean(MyBatchClient)
        BookListener bookListener = context.getBean(BookListener)
        bookListener.books?.clear()

        when:
        myBatchClient.sendBooksAndHeaders([new Book(title: "The Header"), new Book(title: "The Shining")])

        then:
        conditions.eventually {
            bookListener.books.size() == 2
            !bookListener.headers.isEmpty()
            bookListener.headers.every() { it == "Bar" }
            bookListener.books.contains(new Book(title: "The Header"))
            bookListener.books.contains(new Book(title: "The Shining"))
        }
    }

    void "test send batch list - blocking"() {
        given:
        MyBatchClient myBatchClient = context.getBean(MyBatchClient)
        BookListener bookListener = context.getBean(BookListener)
        bookListener.books?.clear()

        when:
        myBatchClient.sendBooks([new Book(title: "The Stand"), new Book(title: "The Shining")])

        then:
        conditions.eventually {
            bookListener.books.size() == 2
            bookListener.books.contains(new Book(title: "The Stand"))
            bookListener.books.contains(new Book(title: "The Shining"))
        }
    }

    void "test send and forward batch list - blocking"() {
        given:
        MyBatchClient myBatchClient = context.getBean(MyBatchClient)
        BookListener bookListener = context.getBean(BookListener)
        TitleListener titleListener = context.getBean(TitleListener)

        bookListener.books?.clear()

        when:
        myBatchClient.sendAndForwardBooks([new Book(title: "It"), new Book(title: "Gerald's Game")])

        then:
        conditions.eventually {
            bookListener.books.size() == 2
            bookListener.books.contains(new Book(title: "It"))
            bookListener.books.contains(new Book(title: "Gerald's Game"))
            titleListener.titles.contains("It")
            titleListener.titles.contains("Gerald's Game")
        }
    }

    void "test send batch array - blocking"() {
        given:
        MyBatchClient myBatchClient = context.getBean(MyBatchClient)
        BookListener bookListener = context.getBean(BookListener)
        bookListener.books?.clear()

        when:
        myBatchClient.sendBooks(new Book(title: "Along Came a Spider"), new Book(title: "The Watchmen"))

        then:
        conditions.eventually {
            bookListener.books.size() == 2
            bookListener.books.contains(new Book(title: "Along Came a Spider"))
            bookListener.books.contains(new Book(title: "The Watchmen"))
        }
    }

    void "test send and forward batch array - blocking"() {
        given:
        MyBatchClient myBatchClient = context.getBean(MyBatchClient)
        BookListener bookListener = context.getBean(BookListener)
        TitleListener titleListener = context.getBean(TitleListener)

        bookListener.books?.clear()

        when:
        myBatchClient.sendAndForward(new Book(title: "Pillars of the Earth"), new Book(title: "War of the World"))

        then:
        conditions.eventually {
            bookListener.books.size() == 2
            bookListener.books.contains(new Book(title: "Pillars of the Earth"))
            bookListener.books.contains(new Book(title: "War of the World"))
            titleListener.titles.contains("Pillars of the Earth")
            titleListener.titles.contains("War of the World")
        }
    }

    void "test send and forward batch flux"() {
        given:
        MyBatchClient myBatchClient = context.getBean(MyBatchClient)
        BookListener bookListener = context.getBean(BookListener)
        bookListener.books?.clear()

        when:
        Flux<Book> results = myBatchClient.sendAndForwardFlux(Flux.fromIterable([new Book(title: "The Stand"), new Book(title: "The Shining")]))
        results.collectList().block()

        then:
        conditions.eventually {
            bookListener.books.size() == 2
            bookListener.books.contains(new Book(title: "The Stand"))
            bookListener.books.contains(new Book(title: "The Shining"))
        }
    }

    void "test send batch flux"() {
        given:
        MyBatchClient myBatchClient = context.getBean(MyBatchClient)
        BookListener bookListener = context.getBean(BookListener)
        bookListener.books?.clear()

        when:
        myBatchClient.sendBooksFlux(Flux.fromIterable([new Book(title: "The Flux"), new Book(title: "The Shining")]))

        then:
        conditions.eventually {
            bookListener.books.size() == 2
            bookListener.books.contains(new Book(title: "The Flux"))
            bookListener.books.contains(new Book(title: "The Shining"))
        }
    }

    void "test keys and values deserialized to the correct type when receiving a batch of ConsumerRecord"() {
        given:
        MyBatchClient myBatchClient = context.getBean(MyBatchClient)
        BookListener bookListener = context.getBean(BookListener)
        bookListener.books?.clear()
        bookListener.keys?.clear()

        when:
        myBatchClient.sendToReceiveAsConsumerRecord("book-1", new Book(title: "The Flowable"))
        myBatchClient.sendToReceiveAsConsumerRecord("book-2", new Book(title: "The Shining"))

        then:
        conditions.eventually {
            bookListener.books == [new Book(title: "The Flowable"), new Book(title: "The Shining")]
            bookListener.keys == ["book-1", "book-2"]
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaBatchListenerSpec')
    @KafkaClient(batch = true)
    @Topic(KafkaBatchListenerSpec.BOOKS_TOPIC)
    static interface MyBatchClient {

        @Topic(KafkaBatchListenerSpec.BOOKS_LIST_TOPIC)
        void sendBooks(List<Book> books)

        @Topic(KafkaBatchListenerSpec.BOOKS_HEADERS_TOPIC)
        @MessageHeader(name = "X-Foo", value = "Bar")
        void sendBooksAndHeaders(List<Book> books)

        @Topic(KafkaBatchListenerSpec.BOOKS_FORWARD_LIST_TOPIC)
        void sendAndForwardBooks(List<Book> books)

        @Topic(KafkaBatchListenerSpec.BOOKS_ARRAY_TOPIC)
        void sendBooks(Book... books)

        @Topic(KafkaBatchListenerSpec.BOOKS_FORWARD_ARRAY_TOPIC)
        void sendAndForward(Book... books)

        @Topic(KafkaBatchListenerSpec.BOOKS_FORWARD_FLUX_TOPIC)
        Flux<Book> sendAndForwardFlux(Flux<Book> books)

        @Topic(KafkaBatchListenerSpec.BOOKS_FLUX_TOPIC)
        void sendBooksFlux(Flux<Book> books)

        @Topic(KafkaBatchListenerSpec.BOOK_CONSUMER_RECORD_LIST_TOPIC)
        void sendToReceiveAsConsumerRecord(@KafkaKey String key, @MessageBody Book book)

    }

    @Requires(property = 'spec.name', value = 'KafkaBatchListenerSpec')
    @KafkaListener(batch = true, offsetReset = EARLIEST)
    @Topic(KafkaBatchListenerSpec.BOOKS_TOPIC)
    static class BookListener {
        List<Book> books = []
        List<String> keys = []
        List<String> headers = []

        @Topic(KafkaBatchListenerSpec.BOOKS_LIST_TOPIC)
        void receiveList(List<Book> books) {
            this.books.addAll books
        }

        @Topic(KafkaBatchListenerSpec.BOOKS_HEADERS_TOPIC)
        void receiveList(List<Book> books, @MessageHeader("X-Foo") List<String> foos) {
            this.books.addAll books
            this.headers = foos
        }

        @Topic(KafkaBatchListenerSpec.BOOKS_ARRAY_TOPIC)
        void receiveArray(Book... books) {
            this.books.addAll Arrays.asList(books)
        }

        @Topic(KafkaBatchListenerSpec.BOOKS_FORWARD_LIST_TOPIC)
        @SendTo(KafkaBatchListenerSpec.TITLES_TOPIC)
        List<String> receiveAndSendList(List<Book> books) {
            this.books.addAll(books)
            return books*.title
        }

        @Topic(KafkaBatchListenerSpec.BOOKS_FORWARD_ARRAY_TOPIC)
        @SendTo(KafkaBatchListenerSpec.TITLES_TOPIC)
        String[] receiveAndSendArray(Book... books) {
            this.books.addAll Arrays.asList(books)
            return books*.title as String[]
        }

        @SendTo(KafkaBatchListenerSpec.TITLES_TOPIC)
        @Topic(KafkaBatchListenerSpec.BOOKS_FORWARD_FLUX_TOPIC)
        Flux<String> receiveAndSendFlux(Flux<Book> books) {
            this.books.addAll books.collectList().block()
            return books.map { Book book -> book.title }
        }

        @Topic(KafkaBatchListenerSpec.BOOKS_FLUX_TOPIC)
        void receiveFlux(Flux<Book> books) {
            this.books.addAll books.collectList().block()
        }

        @Topic(KafkaBatchListenerSpec.BOOK_CONSUMER_RECORD_LIST_TOPIC)
        void receiveConsumerRecords(List<ConsumerRecord<String, Book>> books) {
          this.keys.addAll(books.collect { it.key() })
          this.books.addAll(books.collect { it.value() })
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaBatchListenerSpec')
    @KafkaListener(batch = true, offsetReset = EARLIEST)
    static class TitleListener {
        List<String> titles = []

        @Topic(KafkaBatchListenerSpec.TITLES_TOPIC)
        void receiveTitles(String... titles) {
            this.titles.addAll(titles)
        }
    }

    @ToString(includePackage = false)
    @EqualsAndHashCode
    @Serdeable
    static class Book {
        String title
    }
}

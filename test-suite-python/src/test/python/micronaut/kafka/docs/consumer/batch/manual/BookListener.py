from java.lang import Long
from java.util import Collections
from micronaut.configuration.kafka.annotation import KafkaListener, OffsetReset, OffsetStrategy, Topic
from micronaut.context.annotation import Requires
from micronaut.kafka.docs.consumer.batch.Book import Book
from org.apache.kafka.clients.consumer import Consumer, ConsumerRecord, ConsumerRecords, OffsetAndMetadata
from org.apache.kafka.common import TopicPartition


@Requires(property="spec.name", value="BatchManualAckSpec")
class BookListener:

    # tag::method[]
    @KafkaListener(offsetReset=OffsetReset.EARLIEST, offsetStrategy=OffsetStrategy.DISABLED, batch=True)  # <1>
    @Topic("all-the-books")
    def receive(self, records: list[ConsumerRecord[str, Book]], kafka_consumer: Consumer) -> None:  # <2>

        for record in records:  # <3>

            # process the book
            book = record.value()

            # commit offsets
            topic = record.topic()
            partition = record.partition()
            offset = record.offset()  # <4>

            kafka_consumer.commitSync(Collections.singletonMap(  # <5>
                TopicPartition(topic, partition),
                OffsetAndMetadata(offset + 1, "my metadata")
            ))
    # end::method[]

    # tag::consumerRecords[]
    @KafkaListener(offsetReset=OffsetReset.EARLIEST, offsetStrategy=OffsetStrategy.DISABLED, batch=True)  # <1>
    @Topic("all-the-books")
    def receive_consumer_records(self, consumer_records: ConsumerRecords[str, Book], kafka_consumer: Consumer) -> None:  # <2>
        for partition in consumer_records.partitions():  # <3>
            offset = Long.MIN_VALUE
            # process partition records
            for record in consumer_records.records(partition):  # <4>
                # process the book
                book = record.value()
                # keep last offset
                offset = record.offset()  # <5>

            # commit partition offset
            kafka_consumer.commitSync(Collections.singletonMap(  # <6>
                partition,
                OffsetAndMetadata(offset + 1, "my metadata")
            ))
    # end::consumerRecords[]

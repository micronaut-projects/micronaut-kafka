package io.micronaut.kafka.docs.consumer.batch.manual

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.consumer.batch.Book
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import java.util.*
// end::imports[]

@Requires(property = "spec.name", value = "BatchManualAckSpec")
internal class BookListener {

    // tag::method[]
    @KafkaListener(offsetReset = OffsetReset.EARLIEST, offsetStrategy = OffsetStrategy.DISABLED, batch = true) // <1>
    @Topic("all-the-books")
    fun receive(records: List<ConsumerRecord<String?, Book?>>, kafkaConsumer: Consumer<*, *>) { // <2>
        for (i in records.indices) {
            val record = records[i] // <3>

            // process the book
            val book = record.value()

            // commit offsets
            val topic = record.topic()
            val partition = record.partition()
            val offset = record.offset() // <4>
            kafkaConsumer.commitSync(
                Collections.singletonMap( // <5>
                    TopicPartition(topic, partition),
                    OffsetAndMetadata(offset + 1, "my metadata")
                )
            )
        }
    }
    // end::method[]
}


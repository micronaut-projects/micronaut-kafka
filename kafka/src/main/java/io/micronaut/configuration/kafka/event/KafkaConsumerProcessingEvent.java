package io.micronaut.configuration.kafka.event;

import org.apache.kafka.clients.consumer.Consumer;

/**
 * An event fired after a Kafka consumer beings processing records
 */
public final class KafkaConsumerProcessingEvent extends AbstractKafkaApplicationEvent<Consumer>{

  public KafkaConsumerProcessingEvent(Consumer consumer) {
    super(consumer);
  }
}

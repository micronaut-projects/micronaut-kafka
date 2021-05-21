package example;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.messaging.annotation.MessageBody;

@KafkaClient
public interface TasksProducer {

    @Topic(TaskConstants.TOPIC)
    void send(@MessageBody Task body);

}

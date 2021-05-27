package example;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.messaging.annotation.MessageBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

@KafkaListener(offsetReset = OffsetReset.EARLIEST)
public class TasksListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(TasksListener.class);

    public static final AtomicInteger TASKS_PROCESSED = new AtomicInteger();

    @Topic(TaskConstants.TOPIC)
    public void receive(@MessageBody Task task) {
        LOGGER.info("Received task with id: " + task.getId());
        TASKS_PROCESSED.incrementAndGet();
    }

}
package example;

import io.micronaut.scheduling.annotation.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Singleton;
import java.util.concurrent.atomic.AtomicInteger;

@Singleton
public class TasksPublisher {

    private final static Logger LOGGER = LoggerFactory.getLogger(TasksPublisher.class);
    private final AtomicInteger tasksIds = new AtomicInteger();
    private final TasksProducer tasksProducer;

    public TasksPublisher(TasksProducer tasksProducer) {
        this.tasksProducer = tasksProducer;
    }

    @Scheduled(initialDelay = "2s", fixedRate = "1s")
    public void sendMessages() {
        try {
            int id = tasksIds.incrementAndGet();
            LOGGER.info("Publishing a task with id: " + id);
            tasksProducer.send(new Task(id));
        } catch (Exception e) {
            LOGGER.error("Failed to publish a task", e);
        }
    }

}

package example;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

@Controller("/tasks")
public class TasksController {

    @Get("/processed-count")
    public int get() {
        return TasksListener.TASKS_PROCESSED.get();
    }

    @Get("/unique-group-id-delete-on-shutdown")
    public int uniqueGroupIdDeleteOnShutdown() {
        return ConsumerGroupIdDeleteOnShutDownTaskListener.TASKS_PROCESSED.get();
    }
}

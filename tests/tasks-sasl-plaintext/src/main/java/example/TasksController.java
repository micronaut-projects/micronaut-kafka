package example;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

@Controller("/tasks")
public class TasksController {

    @Get("/processed-count")
    public int get() {
        return TasksListener.TASKS_PROCESSED.get();
    }

}

package example;

import io.micronaut.core.annotation.Introspected;

@Introspected
public class Task {

    private final int id;

    public Task(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }
}

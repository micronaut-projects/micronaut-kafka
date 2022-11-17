package example;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public class Task {

    private final int id;

    public Task(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }
}

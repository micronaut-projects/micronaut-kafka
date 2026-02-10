package example;

import io.micronaut.core.annotation.NonNull;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import io.micronaut.testcontainers.kafka.Kafka;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TasksTest implements TestPropertyProvider {

    @Test
    void testKafka(@Client("/")
                   HttpClient client) {
        await().atMost(30, SECONDS).until(() ->
            {
                Integer result = client.toBlocking().retrieve("/tasks/processed-count", Integer.class);
                return result != null && result > 3;
            }
        );
    }

    @Test
    void uniqueGroupIdDeleteOnShutdown(@Client("/") HttpClient client) {
        await().atMost(30, SECONDS).until(() -> {
                Integer result = client.toBlocking().retrieve("/tasks/unique-group-id-delete-on-shutdown", Integer.class);
                return result != null && result > 3;
            }
        );
    }

    @Override
    public @NonNull Map<String, String> getProperties() {
       return Kafka.getProperties();
    }
}

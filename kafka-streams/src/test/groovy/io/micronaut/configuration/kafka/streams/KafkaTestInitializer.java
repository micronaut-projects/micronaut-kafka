package io.micronaut.configuration.kafka.streams;

import io.micronaut.configuration.kafka.streams.optimization.OptimizationStream;
import io.micronaut.configuration.kafka.streams.startkafkastreams.StartKafkaStreamsOff;
import io.micronaut.configuration.kafka.streams.wordcount.WordCountStream;
import io.micronaut.context.annotation.BootstrapContextCompatible;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.env.BootstrapPropertySourceLocator;
import io.micronaut.context.env.Environment;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.exceptions.ConfigurationException;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@BootstrapContextCompatible
@Singleton
public class KafkaTestInitializer implements BootstrapPropertySourceLocator {

    private final Map<String, Object> adminProps;

    public KafkaTestInitializer(@Value("${kafka.bootstrap.servers}") String bootstrapServers) {
        this.adminProps = Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    @PostConstruct
    void initializeTopics() {

        createTopics(Stream.of(WordCountStream.INPUT,
            WordCountStream.OUTPUT,
            WordCountStream.NAMED_WORD_COUNT_INPUT,
            WordCountStream.NAMED_WORD_COUNT_OUTPUT,
            StartKafkaStreamsOff.STREAMS_OFF_INPUT,
            StartKafkaStreamsOff.STREAMS_OFF_OUTPUT,
            OptimizationStream.OPTIMIZATION_ON_INPUT,
            OptimizationStream.OPTIMIZATION_OFF_INPUT).map(topicName -> configureTopic(topicName, 1, 1)).collect(Collectors.toSet()));
    }

    @Override
    public Iterable<PropertySource> findPropertySources(Environment environment) throws ConfigurationException {
        return BootstrapPropertySourceLocator.EMPTY_LOCATOR.findPropertySources(environment);
    }

    private NewTopic configureTopic(String name, int numPartitions, int replicationFactor) {
        return new NewTopic(name, numPartitions, (short) replicationFactor);
    }

    private void createTopics(Set<NewTopic> topicsToCreate) {
        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> existingTopics = admin.listTopics().names().get();
            Set<NewTopic> newTopics = topicsToCreate.stream().filter(newTopic -> !existingTopics.contains(newTopic.name())).collect(Collectors.toSet());
            admin.createTopics(newTopics).all().get();
        } catch (ExecutionException | InterruptedException e) {
            throw new IllegalStateException("Failed to initialize test kafka topics", e);
        }
    }
}

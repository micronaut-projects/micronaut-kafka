package io.micronaut.kafka.docs.streams

import io.micronaut.context.annotation.BootstrapContextCompatible
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import io.micronaut.context.env.BootstrapPropertySourceLocator
import io.micronaut.context.env.Environment
import io.micronaut.context.env.PropertySource
import io.micronaut.context.exceptions.ConfigurationException
import jakarta.annotation.PostConstruct
import jakarta.inject.Singleton
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import java.util.*
import java.util.concurrent.ExecutionException
import java.util.stream.Collectors
import java.util.stream.Stream

@Requires(property = "spec.name", value = "WordCountStreamTest")
@BootstrapContextCompatible
@Singleton
class KafkaTestInitializer(@Value("\${kafka.bootstrap.servers}") bootstrapServers: String) :
    BootstrapPropertySourceLocator {
    private val adminProps: Map<String, Any>

    init {
        adminProps = Collections.singletonMap<String, Any>(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    }

    @PostConstruct
    fun initializeTopics() {
        createTopics(
            Stream.of(
                "streams-plaintext-input",
                "named-word-count-input",
                "my-other-stream",
                "no-op-input"
            ).map { topicName: String -> configureTopic(topicName, 1, 1) }.collect(Collectors.toSet())
        )
    }

    @Throws(ConfigurationException::class)
    override fun findPropertySources(environment: Environment): Iterable<PropertySource> {
        return BootstrapPropertySourceLocator.EMPTY_LOCATOR.findPropertySources(environment)
    }

    private fun configureTopic(name: String, numPartitions: Int, replicationFactor: Int): NewTopic {
        return NewTopic(name, numPartitions, replicationFactor.toShort())
    }

    private fun createTopics(topicsToCreate: Set<NewTopic>) {
        try {
            AdminClient.create(adminProps).use { admin ->
                val existingTopics = admin.listTopics().names().get()
                val newTopics = topicsToCreate.stream().filter { newTopic: NewTopic ->
                        !existingTopics.contains(newTopic.name()) }.collect(Collectors.toSet())
                admin.createTopics(newTopics).all().get()
            }
        } catch (e: ExecutionException) {
            throw IllegalStateException("Failed to initialize test kafka topics", e)
        } catch (e: InterruptedException) {
            throw IllegalStateException("Failed to initialize test kafka topics", e)
        }
    }
}

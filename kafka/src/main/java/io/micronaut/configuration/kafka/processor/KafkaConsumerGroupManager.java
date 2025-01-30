/*
 * Copyright 2017-2025 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.kafka.processor;

import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.runtime.event.ApplicationShutdownEvent;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A singleton bean responsible for managing Kafka consumer groups during the application lifecycle.
 * <p>
 * This class listens for the application shutdown event and deletes any Kafka consumer groups
 * marked for removal.
 * </p>
 *
 *
 * <p>When the application shuts down, any Consumer added using
 * {@link #registerConsumerForGroupDeletion(String, ConsumerState)}
 * will be deleted from Kafka.</p>
 *
 * @author Khan, C M Abdullah
 * @since 5.7
 */

@Singleton
@Requires(bean = AdminClient.class)
class KafkaConsumerGroupManager implements ApplicationEventListener<ApplicationShutdownEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerGroupManager.class);

    private final AdminClient adminClient;
    /**
     * Stores the client IDs and their associated consumer states for consumer groups to be
     * deleted on shutdown.
     */
    private final Map<String, ConsumerState> registerConsumerForGroupDeletion =
        new ConcurrentHashMap<>();

    /**
     * List of unique consumer group IDs that are scheduled for deletion on shutdown.
     */
    private final List<String> uniqueGroupIdsDeleteOnShutdown = new ArrayList<>();

    /**
     * Constructs a KafkaConsumerGroupManager with the provided {@link AdminClient}.
     *
     * @param adminClient The Kafka {@link AdminClient} used for managing consumer groups.
     */
    public KafkaConsumerGroupManager(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    /**
     * Handles the application shutdown event by closing consumers and deleting their associated
     * consumer groups.
     *
     * @param event The application shutdown event.
     */
    @Override
    public void onApplicationEvent(ApplicationShutdownEvent event) {
        LOG.info("Application shutdown initiated. Preparing to delete registered Kafka unique consumer groups.");
        if (!uniqueGroupIdsDeleteOnShutdown.isEmpty()) {
            LOG.info("Closing {} consumers and attempting to delete the following consumer groups: {}",
                uniqueGroupIdsDeleteOnShutdown.size(), uniqueGroupIdsDeleteOnShutdown);
            closeConsumers();
            adminClient.deleteConsumerGroups(uniqueGroupIdsDeleteOnShutdown)
                .all().whenComplete((voidResult, throwable) -> {
                    if (throwable == null) {
                        LOG.info("Successfully deleted the following consumer groups: {}", uniqueGroupIdsDeleteOnShutdown);
                    } else {
                        LOG.warn("Failed to delete the following consumer groups: {}. Error: {}",
                            uniqueGroupIdsDeleteOnShutdown, throwable.getMessage(), throwable);
                    }
                });
        } else {
            LOG.info("No unique consumer groups are registered for deletion.");
        }
    }

    /**
     * Closes all consumers and clears the list of consumers pending group deletion.
     */
    private void closeConsumers() {
        LOG.info("Closing all registered Kafka consumers who has unique group id.");
        registerConsumerForGroupDeletion.values().forEach(ConsumerState::wakeUp);
        registerConsumerForGroupDeletion.values().forEach(ConsumerState::close);
        registerConsumerForGroupDeletion.clear();
        LOG.info("All registered Kafka consumers who have unique group IDs have been successfully closed.");
    }

    /**
     * Registers a Kafka consumer for its group to be deleted on shutdown.
     *
     * @param clientId      The client ID of the consumer.
     * @param consumerState The state of the consumer to be registered.
     */
    void registerConsumerForGroupDeletion(String clientId, ConsumerState consumerState) {
        if (clientId != null && !clientId.isEmpty() && consumerState != null) {
            registerConsumerForGroupDeletion.put(clientId, consumerState);
            LOG.info("Registered consumer with client ID '{}' for group deletion on shutdown.",
                clientId);
        } else {
            LOG.warn("Failed to register consumer. Either client ID is null/empty or consumer state is null.");
        }
    }

    /**
     * Registers a consumer group ID to be deleted upon application shutdown.
     *
     * @param groupId The Kafka consumer group ID to be scheduled for deletion.
     */
    public void registerConsumerGroupIdForDeletion(String groupId) {
        if (groupId != null && !groupId.isEmpty()) {
            uniqueGroupIdsDeleteOnShutdown.add(groupId);
            LOG.info("Registered consumer group ID for deletion: {}", groupId);
        } else {
            LOG.warn("Attempted to register a null or empty consumer group ID for deletion");
        }
    }

    /**
     * Retrieves the list of client IDs that are currently registered for group deletion.
     *
     * @return A list of registered client IDs.
     */
    List<String> getRegisteredClientIdsForDeletion() {
        return registerConsumerForGroupDeletion.keySet().stream().toList();
    }
}

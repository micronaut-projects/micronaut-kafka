/*
 * Copyright 2017-2020 original authors
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
package io.micronaut.configuration.kafka.admin;

import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.annotation.Experimental;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Creates Kafka topics via {@link AdminClient}.
 *
 * @author Guillermo Calvo
 * @since 5.2
 */
@Context
@Requires(bean = AdminClient.class)
@Requires(bean = NewTopic.class)
public class KafkaNewTopics {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaNewTopics.class);

    @NonNull
    private final CreateTopicsResult result;

    /**
     * @param adminClient The Kafka admin client.
     * @param options Optional {@link CreateTopicsOptions}.
     * @param topics  The list of {@link NewTopic} beans to create.
     */
    public KafkaNewTopics(
        @NonNull AdminClient adminClient,
        @Nullable CreateTopicsOptions options,
        @NonNull List<NewTopic> topics
    ) {
        LOG.info("Creating new topics: {}", topics);
        this.result = adminClient.createTopics(topics, options != null ? options : new CreateTopicsOptions());
    }

    /**
     * @return The {@link CreateTopicsResult}.
     */
    @Experimental
    @NonNull
    public CreateTopicsResult getResult() {
        return result;
    }
}

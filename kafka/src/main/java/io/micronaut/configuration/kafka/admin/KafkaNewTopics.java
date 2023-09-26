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

import io.micronaut.context.ApplicationContext;
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
import java.util.Optional;

import static java.util.function.Predicate.not;

/**
 * Creates Kafka topics via {@link AdminClient}.
 *
 * @author Guillermo Calvo
 * @since 5.2
 */
@Context
@Requires(bean = AdminClient.class)
public class KafkaNewTopics {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaNewTopics.class);

    @Nullable
    private final CreateTopicsOptions options;

    @Nullable
    private final CreateTopicsResult result;

    /**
     * @param context The application context.
     * @param options Optional {@link CreateTopicsOptions}.
     * @param topics  Optional list of {@link NewTopic} beans.
     */
    public KafkaNewTopics(
        @NonNull ApplicationContext context,
        @Nullable CreateTopicsOptions options,
        @Nullable List<NewTopic> topics
    ) {
        this.options = options != null ? options : new CreateTopicsOption();
        this.result = Optional.ofNullable(topics).filter(not(List::isEmpty))
            .map(t -> createNewTopics(context, t))
            .orElse(null);
    }

    /**
     * @return An optional {@link CreateTopicsResult} if new topics were created.
     */
    @Experimental
    public Optional<CreateTopicsResult> getResult() {
        return Optional.ofNullable(result);
    }

    private CreateTopicsResult createNewTopics(@NonNull ApplicationContext context, @NonNull List<NewTopic> topics) {
        LOG.info("Creating new topics: {}", topics);
        final AdminClient adminClient = context.getBean(AdminClient.class);
        return adminClient.createTopics(topics, options);
    }
}

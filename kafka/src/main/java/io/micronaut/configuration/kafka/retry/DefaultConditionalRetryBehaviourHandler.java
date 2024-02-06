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
package io.micronaut.configuration.kafka.retry;

import io.micronaut.configuration.kafka.exceptions.KafkaListenerException;
import io.micronaut.context.annotation.Primary;
import jakarta.inject.Singleton;

/**
 * The default ConditionalRetryBehaviourHandler used when a {@link org.apache.kafka.clients.consumer.KafkaConsumer}
 * fails to process a {@link org.apache.kafka.clients.consumer.ConsumerRecord} and the error strategy is set to conditionally retry.
 *
 * @author Ryan Evans
 * @since 5.4.0
 */
@Singleton
@Primary
public class DefaultConditionalRetryBehaviourHandler implements ConditionalRetryBehaviourHandler {

    /**
     * Retries the record by default. This can be overridden by implementing a
     * custom ConditionalRetryBehaviourHandler that replaces this bean.
     */
    @Override
    public ConditionalRetryBehaviour conditionalRetryBehaviour(KafkaListenerException exception) {
        return ConditionalRetryBehaviour.RETRY;
    }
}

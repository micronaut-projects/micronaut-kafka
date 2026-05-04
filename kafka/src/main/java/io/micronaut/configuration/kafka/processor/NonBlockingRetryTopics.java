/*
 * Copyright 2017-2026 original authors
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

import io.micronaut.configuration.kafka.annotation.ErrorStrategy;
import io.micronaut.configuration.kafka.annotation.ErrorStrategyValue;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.util.CollectionUtils;
import io.micronaut.core.util.StringUtils;
import io.micronaut.messaging.exceptions.MessagingSystemException;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Non-blocking retry topic metadata resolved from a listener definition.
 *
 * @author Micronaut Framework Team
 * @since 6.0.0
 */
@Internal
final class NonBlockingRetryTopics {

    private final List<String> suffixes;
    private final List<Duration> delays;
    private final Map<String, TopicBinding> bindings;

    private NonBlockingRetryTopics(List<String> suffixes, List<Duration> delays, Map<String, TopicBinding> bindings) {
        this.suffixes = suffixes;
        this.delays = delays;
        this.bindings = bindings;
    }

    @Nullable
    static NonBlockingRetryTopics create(
        AnnotationValue<KafkaListener> kafkaListener,
        List<AnnotationValue<Topic>> topicAnnotations,
        boolean batch
    ) {
        Optional<AnnotationValue<ErrorStrategy>> errorStrategyAnnotation = kafkaListener.getAnnotation("errorStrategy", ErrorStrategy.class);
        if (errorStrategyAnnotation.isEmpty()) {
            return null;
        }
        AnnotationValue<ErrorStrategy> errorStrategy = errorStrategyAnnotation.get();
        ErrorStrategyValue errorStrategyValue = errorStrategy.getRequiredValue(ErrorStrategyValue.class);
        if (!errorStrategyValue.isRetryTopic()) {
            return null;
        }
        if (batch) {
            throw new MessagingSystemException("Error strategy 'RETRY_TOPIC_ON_ERROR' is not supported for batch Kafka listeners");
        }
        List<String> directTopics = topicAnnotations.stream()
            .flatMap(annotationValue -> Arrays.stream(annotationValue.stringValues()))
            .filter(StringUtils::isNotEmpty)
            .collect(java.util.stream.Collectors.collectingAndThen(
                java.util.stream.Collectors.toCollection(LinkedHashSet::new),
                List::copyOf
            ));
        if (directTopics.isEmpty()) {
            throw new MessagingSystemException("Error strategy 'RETRY_TOPIC_ON_ERROR' requires direct topic names and does not support topic patterns");
        }
        boolean hasPatterns = topicAnnotations.stream().anyMatch(annotationValue -> annotationValue.stringValues("patterns").length > 0);
        if (hasPatterns) {
            throw new MessagingSystemException("Error strategy 'RETRY_TOPIC_ON_ERROR' does not support @Topic patterns");
        }
        List<String> suffixes = Arrays.stream(errorStrategy.stringValues("retryTopicSuffixes"))
            .filter(StringUtils::isNotEmpty)
            .toList();
        if (suffixes.isEmpty()) {
            throw new MessagingSystemException("Error strategy 'RETRY_TOPIC_ON_ERROR' requires at least one retry topic suffix");
        }
        List<String> delayStrings = Arrays.stream(errorStrategy.stringValues("retryTopicDelays"))
            .filter(StringUtils::isNotEmpty)
            .toList();
        if (delayStrings.size() != suffixes.size()) {
            throw new MessagingSystemException("Error strategy 'RETRY_TOPIC_ON_ERROR' requires the same number of retry topic suffixes and retry topic delays");
        }
        List<Duration> delays = new ArrayList<>(delayStrings.size());
        for (String delayString : delayStrings) {
            Duration delay = ConversionService.SHARED.convert(delayString, Duration.class)
                .filter(duration -> !duration.isZero() && !duration.isNegative())
                .orElseThrow(() -> new MessagingSystemException("Invalid retry topic delay [" + delayString + "] for error strategy 'RETRY_TOPIC_ON_ERROR'"));
            delays.add(delay);
        }
        Map<String, TopicBinding> bindings = new LinkedHashMap<>();
        for (String directTopic : directTopics) {
            TopicBinding originalBinding = new TopicBinding(directTopic, 0);
            bindings.put(directTopic, originalBinding);
            for (int i = 0; i < suffixes.size(); i++) {
                String retryTopic = directTopic + suffixes.get(i);
                TopicBinding previous = bindings.putIfAbsent(retryTopic, new TopicBinding(directTopic, i + 1));
                if (previous != null && !previous.originalTopic().equals(directTopic)) {
                    throw new MessagingSystemException("Retry topic [" + retryTopic + "] maps to multiple original topics");
                }
            }
        }
        return new NonBlockingRetryTopics(List.copyOf(suffixes), List.copyOf(delays), Map.copyOf(bindings));
    }

    List<String> expandTopics(String[] topics) {
        List<String> expandedTopics = new ArrayList<>(topics.length * (suffixes.size() + 1));
        for (String topic : topics) {
            expandedTopics.add(topic);
            for (String suffix : suffixes) {
                expandedTopics.add(topic + suffix);
            }
        }
        return List.copyOf(expandedTopics);
    }

    List<String> additionalRetryTopics() {
        return bindings.entrySet().stream()
            .filter(entry -> entry.getValue().attempt() > 0)
            .map(Map.Entry::getKey)
            .toList();
    }

    boolean isRetryTopic(String topic) {
        TopicBinding binding = bindings.get(topic);
        return binding != null && binding.attempt() > 0;
    }

    @Nullable
    RetryDispatch nextRetry(String topic) {
        TopicBinding binding = bindings.get(topic);
        if (binding == null) {
            return null;
        }
        int nextRetryIndex = binding.attempt();
        if (nextRetryIndex >= suffixes.size()) {
            return null;
        }
        return new RetryDispatch(
            binding.originalTopic(),
            binding.originalTopic() + suffixes.get(nextRetryIndex),
            delays.get(nextRetryIndex),
            nextRetryIndex + 1
        );
    }

    @Nullable
    TopicBinding binding(String topic) {
        return bindings.get(topic);
    }

    boolean applies() {
        return CollectionUtils.isNotEmpty(bindings);
    }

    record TopicBinding(String originalTopic, int attempt) {
    }

    record RetryDispatch(String originalTopic, String retryTopic, Duration delay, int attempt) {
    }
}

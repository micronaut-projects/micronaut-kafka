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

import io.micronaut.core.annotation.Internal;
import io.micronaut.messaging.exceptions.MessagingSystemException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Resolves values by topic name or topic pattern.
 *
 * @param <T> The routed value type
 * @author Micronaut Framework Team
 * @since 6.0.0
 */
@Internal
final class TopicRouter<T> {

    private final Map<String, T> directRoutes = new LinkedHashMap<>();
    private final List<Route<T>> routes = new ArrayList<>();

    void register(String[] topics, String[] patterns, T value, String source) {
        try {
            Route<T> route = new Route<>(topics, patterns, value, source);
            for (String topic : route.topics()) {
                T existing = directRoutes.putIfAbsent(topic, value);
                if (existing != null && existing != value) {
                    throw new MessagingSystemException("Topic [" + topic + "] is already mapped to a different route");
                }
            }
            routes.add(route);
        } catch (PatternSyntaxException e) {
            throw new MessagingSystemException("Invalid topic pattern [" + e.getPattern() + "] for [" + source + "]: " + e.getMessage(), e);
        }
    }

    T resolve(String topic, String purpose) {
        T direct = directRoutes.get(topic);
        if (direct != null) {
            List<String> patternMatches = new ArrayList<>(2);
            for (Route<T> route : routes) {
                if (!route.matchesPattern(topic)) {
                    continue;
                }
                if (route.value() != direct) {
                    patternMatches.add(route.source());
                }
            }
            if (!patternMatches.isEmpty()) {
                throw new MessagingSystemException("Topic [" + topic + "] has a direct " + purpose + " route and also matches pattern " + purpose + " routes: " + String.join(", ", patternMatches));
            }
            return direct;
        }
        T resolved = null;
        List<String> matches = new ArrayList<>(2);
        for (Route<T> route : routes) {
            if (!route.matchesPattern(topic)) {
                continue;
            }
            matches.add(route.source());
            if (resolved != null && resolved != route.value()) {
                throw new MessagingSystemException("Topic [" + topic + "] matches multiple " + purpose + " routes: " + String.join(", ", matches));
            }
            resolved = route.value();
        }
        if (resolved == null) {
            throw new MessagingSystemException("No " + purpose + " route found for topic [" + topic + "]");
        }
        return resolved;
    }

    Collection<String> topics() {
        return Collections.unmodifiableSet(new LinkedHashSet<>(directRoutes.keySet()));
    }

    Collection<String> patterns() {
        LinkedHashSet<String> patterns = new LinkedHashSet<>();
        for (Route<T> route : routes) {
            patterns.addAll(route.patternStrings());
        }
        return Collections.unmodifiableSet(patterns);
    }

    boolean hasPatterns() {
        return routes.stream().anyMatch(Route::hasPatterns);
    }

    Collection<T> values() {
        LinkedHashSet<T> values = new LinkedHashSet<>();
        for (Route<T> route : routes) {
            values.add(route.value());
        }
        return Collections.unmodifiableSet(values);
    }

    List<Route<T>> routes() {
        return Collections.unmodifiableList(routes);
    }

    record Route<T>(List<String> topics, List<String> patternStrings, List<Pattern> patterns, T value, String source) {
        Route(String[] topics, String[] patterns, T value, String source) {
            this(List.of(topics), List.of(patterns), compile(patterns), value, source);
        }

        boolean hasPatterns() {
            return !patterns.isEmpty();
        }

        boolean matchesPattern(String topic) {
            for (Pattern pattern : patterns) {
                if (pattern.matcher(topic).matches()) {
                    return true;
                }
            }
            return false;
        }

        private static List<Pattern> compile(String[] patterns) {
            List<Pattern> compiled = new ArrayList<>(patterns.length);
            for (String pattern : patterns) {
                compiled.add(Pattern.compile(pattern));
            }
            return compiled;
        }
    }
}

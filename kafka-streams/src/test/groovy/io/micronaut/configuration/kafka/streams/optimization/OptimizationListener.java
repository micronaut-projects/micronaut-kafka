/*
 * Copyright 2017-2019 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.kafka.streams.optimization;

import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;

@KafkaListener(offsetReset = OffsetReset.EARLIEST)
public class OptimizationListener {

    private int optimizationOffChangelogMessageCount = 0;

    private int optimizationOnChangelogMessageCount = 0;

    private final String optimizationOffChangelogTopicName =
            OptimizationStream.STREAM_OPTIMIZATION_OFF + "-" +
                    OptimizationStream.OPTIMIZATION_OFF_STORE + "-changelog";

    private final String optimizationOnChangelogTopicName =
            OptimizationStream.STREAM_OPTIMIZATION_ON + "-" +
                    OptimizationStream.OPTIMIZATION_ON_STORE + "-changelog";

    @Topic(optimizationOffChangelogTopicName)
    void optOffCount(@KafkaKey String key, String value) {
        optimizationOffChangelogMessageCount++;
    }

    @Topic(optimizationOnChangelogTopicName)
    void optOnCount(@KafkaKey String key, String value) {
        optimizationOnChangelogMessageCount++;
    }

    public int getOptimizationOffChangelogMessageCount() {
        return optimizationOffChangelogMessageCount;
    }

    public int getOptimizationOnChangelogMessageCount() {
        return optimizationOnChangelogMessageCount;
    }

}

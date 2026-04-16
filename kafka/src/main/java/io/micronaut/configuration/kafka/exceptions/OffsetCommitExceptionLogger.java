/*
 * Copyright 2017-2024 original authors
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
package io.micronaut.configuration.kafka.exceptions;

import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.Nullable;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Collection;

/**
 * Helper for handling Kafka offset commit failures that can be expected during cooperative rebalancing.
 *
 * @since 5.4
 */
@Internal
public final class OffsetCommitExceptionLogger {
    private static final String COOPERATIVE_STICKY_ASSIGNOR = CooperativeStickyAssignor.class.getName();
    private static final String COOPERATIVE_STICKY_ASSIGNOR_SIMPLE_NAME = CooperativeStickyAssignor.class.getSimpleName();

    private OffsetCommitExceptionLogger() {
    }

    public static boolean isCooperativeStickyAssignor(@Nullable Object assignmentStrategy) {
        if (assignmentStrategy == null) {
            return false;
        }
        if (assignmentStrategy instanceof CharSequence value) {
            return Arrays.stream(value.toString().split(","))
                .map(String::trim)
                .anyMatch(OffsetCommitExceptionLogger::isCooperativeStickyAssignorName);
        }
        if (assignmentStrategy instanceof Class<?> clazz) {
            return isCooperativeStickyAssignorName(clazz.getName()) || isCooperativeStickyAssignorName(clazz.getSimpleName());
        }
        if (assignmentStrategy instanceof Collection<?> collection) {
            return collection.stream().anyMatch(OffsetCommitExceptionLogger::isCooperativeStickyAssignor);
        }
        if (assignmentStrategy instanceof Object[] array) {
            return Arrays.stream(array).anyMatch(OffsetCommitExceptionLogger::isCooperativeStickyAssignor);
        }
        return isCooperativeStickyAssignorName(assignmentStrategy.toString());
    }

    public static void log(Logger logger, boolean cooperativeStickyAssignmentStrategy, String message, Throwable exception, Object... arguments) {
        Object[] logArguments = Arrays.copyOf(arguments, arguments.length + 1);
        logArguments[arguments.length] = exception;
        if (cooperativeStickyAssignmentStrategy) {
            logger.warn(message, logArguments);
        } else {
            logger.error(message, logArguments);
        }
    }

    private static boolean isCooperativeStickyAssignorName(String value) {
        return COOPERATIVE_STICKY_ASSIGNOR.equals(value) || COOPERATIVE_STICKY_ASSIGNOR_SIMPLE_NAME.equals(value);
    }
}

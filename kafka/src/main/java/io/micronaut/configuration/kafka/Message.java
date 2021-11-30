/*
 * Copyright 2017-2021 original authors
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
package io.micronaut.configuration.kafka;

import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;

import java.util.Map;

/**
 * Message payload representation.
 *
 * @author Denis Stepanov
 * @since 4.1.0
 */
public final class Message {

    private final String topic;
    private final Object key;
    private final Object body;
    private final Integer partition;
    private final Long timestamp;
    private final Map<String, Object> headers;

    /**
     * The default contructor.
     *
     * @param topic     The topic
     * @param key       The key
     * @param body      The body
     * @param partition The partition
     * @param timestamp The timestamp
     * @param headers   The headers
     */
    public Message(@Nullable String topic, @Nullable Object key, @Nullable Object body, @Nullable Integer partition,
                   @Nullable Long timestamp, @Nullable Map<String, Object> headers) {
        this.topic = topic;
        this.key = key;
        this.body = body;
        this.partition = partition;
        this.timestamp = timestamp;
        this.headers = headers;
    }

    @Nullable
    public String getTopic() {
        return topic;
    }

    @Nullable
    public Object getKey() {
        return key;
    }

    @Nullable
    public Object getBody() {
        return body;
    }

    @Nullable
    public Integer getPartition() {
        return partition;
    }

    @Nullable
    public Long getTimestamp() {
        return timestamp;
    }

    @Nullable
    public Map<String, Object> getHeaders() {
        return headers;
    }

    /**
     * The message builder.
     */
    public static final class Builder {
        private String topic;
        private Object key;
        private Object body;
        private Integer partition;
        private Long timestamp;
        private Map<String, Object> headers;

        @NonNull
        public static Builder withBody(@Nullable Object body) {
            Builder builder = new Builder();
            builder.body = body;
            return builder;
        }

        @NonNull
        public static Builder withoutBody() {
            Builder builder = new Builder();
            return builder;
        }

        @NonNull
        public Builder topic(@Nullable String topic) {
            this.topic = topic;
            return this;
        }

        @NonNull
        public Builder key(@Nullable Object key) {
            this.key = key;
            return this;
        }

        @NonNull
        public Builder body(@Nullable Object body) {
            this.body = body;
            return this;
        }

        @NonNull
        public Builder header(@Nullable Map<String, Object> headers) {
            this.headers = headers;
            return this;
        }

        @NonNull
        public Builder partition(@Nullable Integer partition) {
            this.partition = partition;
            return this;
        }

        @NonNull
        public Builder timestamp(@Nullable Long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        @NonNull
        public Message build() {
            return new Message(topic, key, body, partition, timestamp, headers);
        }
    }

}

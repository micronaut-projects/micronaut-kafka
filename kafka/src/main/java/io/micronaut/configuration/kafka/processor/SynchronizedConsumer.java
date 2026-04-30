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
package io.micronaut.configuration.kafka.processor;

import io.micronaut.core.annotation.Internal;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.KafkaMetric;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Creates a synchronized {@link Consumer} view that shares a monitor with Micronaut's poll loop.
 * The {@code wakeup} method remains unsynchronized because Kafka documents it as the only thread-safe method.
 *
 * @author graemerocher
 * @since 5.7.0
 */
@Internal
final class SynchronizedConsumer {

    private SynchronizedConsumer() {
    }

    static <K, V> Consumer<K, V> wrap(Consumer<K, V> consumer, Object monitor) {
        return new Wrapper<>(consumer, monitor);
    }

    private static final class Wrapper<K, V> implements Consumer<K, V> {

        private final Consumer<K, V> delegate;
        private final Object monitor;

        Wrapper(Consumer<K, V> delegate, Object monitor) {
            this.delegate = delegate;
            this.monitor = monitor;
        }

        @Override
        public Set<TopicPartition> assignment() {
            synchronized (monitor) {
                return delegate.assignment();
            }
        }

        @Override
        public Set<String> subscription() {
            synchronized (monitor) {
                return delegate.subscription();
            }
        }

        @Override
        public void subscribe(Collection<String> topics) {
            synchronized (monitor) {
                delegate.subscribe(topics);
            }
        }

        @Override
        public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
            synchronized (monitor) {
                delegate.subscribe(topics, callback);
            }
        }

        @Override
        public void assign(Collection<TopicPartition> partitions) {
            synchronized (monitor) {
                delegate.assign(partitions);
            }
        }

        @Override
        public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
            synchronized (monitor) {
                delegate.subscribe(pattern, callback);
            }
        }

        @Override
        public void subscribe(Pattern pattern) {
            synchronized (monitor) {
                delegate.subscribe(pattern);
            }
        }

        @Override
        public void subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener callback) {
            synchronized (monitor) {
                delegate.subscribe(pattern, callback);
            }
        }

        @Override
        public void subscribe(SubscriptionPattern pattern) {
            synchronized (monitor) {
                delegate.subscribe(pattern);
            }
        }

        @Override
        public void unsubscribe() {
            synchronized (monitor) {
                delegate.unsubscribe();
            }
        }

        @Override
        public ConsumerRecords<K, V> poll(Duration timeout) {
            synchronized (monitor) {
                return delegate.poll(timeout);
            }
        }

        @Override
        public void commitSync() {
            synchronized (monitor) {
                delegate.commitSync();
            }
        }

        @Override
        public void commitSync(Duration timeout) {
            synchronized (monitor) {
                delegate.commitSync(timeout);
            }
        }

        @Override
        public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
            synchronized (monitor) {
                delegate.commitSync(offsets);
            }
        }

        @Override
        public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
            synchronized (monitor) {
                delegate.commitSync(offsets, timeout);
            }
        }

        @Override
        public void commitAsync() {
            synchronized (monitor) {
                delegate.commitAsync();
            }
        }

        @Override
        public void commitAsync(OffsetCommitCallback callback) {
            synchronized (monitor) {
                delegate.commitAsync(callback);
            }
        }

        @Override
        public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
            synchronized (monitor) {
                delegate.commitAsync(offsets, callback);
            }
        }

        @Override
        public void registerMetricForSubscription(KafkaMetric metric) {
            synchronized (monitor) {
                delegate.registerMetricForSubscription(metric);
            }
        }

        @Override
        public void unregisterMetricFromSubscription(KafkaMetric metric) {
            synchronized (monitor) {
                delegate.unregisterMetricFromSubscription(metric);
            }
        }

        @Override
        public void seek(TopicPartition partition, long offset) {
            synchronized (monitor) {
                delegate.seek(partition, offset);
            }
        }

        @Override
        public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
            synchronized (monitor) {
                delegate.seek(partition, offsetAndMetadata);
            }
        }

        @Override
        public void seekToBeginning(Collection<TopicPartition> partitions) {
            synchronized (monitor) {
                delegate.seekToBeginning(partitions);
            }
        }

        @Override
        public void seekToEnd(Collection<TopicPartition> partitions) {
            synchronized (monitor) {
                delegate.seekToEnd(partitions);
            }
        }

        @Override
        public long position(TopicPartition partition) {
            synchronized (monitor) {
                return delegate.position(partition);
            }
        }

        @Override
        public long position(TopicPartition partition, Duration timeout) {
            synchronized (monitor) {
                return delegate.position(partition, timeout);
            }
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
            synchronized (monitor) {
                return delegate.committed(partitions);
            }
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, Duration timeout) {
            synchronized (monitor) {
                return delegate.committed(partitions, timeout);
            }
        }

        @Override
        public Uuid clientInstanceId(Duration timeout) {
            synchronized (monitor) {
                return delegate.clientInstanceId(timeout);
            }
        }

        @Override
        public Map<MetricName, ? extends Metric> metrics() {
            synchronized (monitor) {
                return delegate.metrics();
            }
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            synchronized (monitor) {
                return delegate.partitionsFor(topic);
            }
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
            synchronized (monitor) {
                return delegate.partitionsFor(topic, timeout);
            }
        }

        @Override
        public Map<String, List<PartitionInfo>> listTopics() {
            synchronized (monitor) {
                return delegate.listTopics();
            }
        }

        @Override
        public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
            synchronized (monitor) {
                return delegate.listTopics(timeout);
            }
        }

        @Override
        public Set<TopicPartition> paused() {
            synchronized (monitor) {
                return delegate.paused();
            }
        }

        @Override
        public void pause(Collection<TopicPartition> partitions) {
            synchronized (monitor) {
                delegate.pause(partitions);
            }
        }

        @Override
        public void resume(Collection<TopicPartition> partitions) {
            synchronized (monitor) {
                delegate.resume(partitions);
            }
        }

        @Override
        public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
            synchronized (monitor) {
                return delegate.offsetsForTimes(timestampsToSearch);
            }
        }

        @Override
        public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
            synchronized (monitor) {
                return delegate.offsetsForTimes(timestampsToSearch, timeout);
            }
        }

        @Override
        public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
            synchronized (monitor) {
                return delegate.beginningOffsets(partitions);
            }
        }

        @Override
        public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
            synchronized (monitor) {
                return delegate.beginningOffsets(partitions, timeout);
            }
        }

        @Override
        public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
            synchronized (monitor) {
                return delegate.endOffsets(partitions);
            }
        }

        @Override
        public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
            synchronized (monitor) {
                return delegate.endOffsets(partitions, timeout);
            }
        }

        @Override
        public OptionalLong currentLag(TopicPartition topicPartition) {
            synchronized (monitor) {
                return delegate.currentLag(topicPartition);
            }
        }

        @Override
        public ConsumerGroupMetadata groupMetadata() {
            synchronized (monitor) {
                return delegate.groupMetadata();
            }
        }

        @Override
        public void enforceRebalance() {
            synchronized (monitor) {
                delegate.enforceRebalance();
            }
        }

        @Override
        public void enforceRebalance(String reason) {
            synchronized (monitor) {
                delegate.enforceRebalance(reason);
            }
        }

        @Override
        public void close() {
            synchronized (monitor) {
                delegate.close();
            }
        }

        /**
         * @deprecated Use {@link #close(CloseOptions)} instead.
         */
        @Override
        @Deprecated
        public void close(Duration timeout) {
            synchronized (monitor) {
                delegate.close(timeout);
            }
        }

        @Override
        public void close(CloseOptions option) {
            synchronized (monitor) {
                delegate.close(option);
            }
        }

        @Override
        public void wakeup() {
            // wakeup() is the only thread-safe KafkaConsumer method; intentionally not synchronized
            delegate.wakeup();
        }
    }
}

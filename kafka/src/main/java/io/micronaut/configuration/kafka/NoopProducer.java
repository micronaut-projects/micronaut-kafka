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
package io.micronaut.configuration.kafka;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.FutureRecordMetadata;
import org.apache.kafka.clients.producer.internals.ProduceRequestResult;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * No-Op Producer for use when kafka is disabled.
 */
public class NoopProducer implements Producer {
    @Override
    public void initTransactions() {

    }

    @Override
    public void beginTransaction() throws ProducerFencedException {

    }

    @Override
    public void commitTransaction() throws ProducerFencedException {

    }

    @Override
    public void abortTransaction() throws ProducerFencedException {

    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord record) {
        return send(record, null);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord record, Callback callback) {
        TopicPartition topicPartition = new TopicPartition(record.topic(), 0);
        ProduceRequestResult result = new ProduceRequestResult(topicPartition);
        FutureRecordMetadata future = new FutureRecordMetadata(result, 0, RecordBatch.NO_TIMESTAMP,
                0L, 0, 0, Time.SYSTEM);
        result.set(0, 0, null);
        result.done();
        return future;
    }

    @Override
    public void flush() {

    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return null;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void close(Duration timeout) {

    }

    @Override
    public void sendOffsetsToTransaction(Map offsets, ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {

    }

    @Override
    public void sendOffsetsToTransaction(Map offsets, String consumerGroupId) throws ProducerFencedException {

    }
}

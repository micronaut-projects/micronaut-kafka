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
package io.micronaut.configuration.kafka.metrics.builder;

import io.micrometer.core.instrument.binder.BaseUnits;
import io.micronaut.core.annotation.Internal;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A static lookup for registering kafka metrics to meter types through the use of {@link MeterType} and {@link KafkaMetricMeterType}.
 *
 * @author Christian Oestreich
 * @since 1.4.1
 */
@Internal
class KafkaMetricMeterTypeRegistry {
    private static final KafkaMetricMeterType DEFAULT_KAFKA_METRIC_METER_TYPE = new KafkaMetricMeterType();
    private static final String RECORDS = "records";
    private static final String PARTITIONS = "partitions";
    private static final String CONNECTIONS = "connections";
    private static final String JOINS = "joins";
    private static final String SYNCS = "syncs";
    private static final String HEARTBEATS = "heartbeats";
    private static final String REQUESTS = "requests";
    private static final String RESPONSES = "responses";

    private final Map<String, KafkaMetricMeterType> meterTypeMap;

    /**
     * Constructor that will populate the metric name to type map.
     */
    public KafkaMetricMeterTypeRegistry() {
        meterTypeMap = new HashMap<>();
        meterTypeMap.put("records-lag", new KafkaMetricMeterType(MeterType.GAUGE, "The latest lag of the partition", RECORDS));
        meterTypeMap.put("records-lag-avg", new KafkaMetricMeterType(MeterType.GAUGE, "The average lag of the partition", RECORDS));
        meterTypeMap.put("records-lag-max", new KafkaMetricMeterType(MeterType.GAUGE, "The maximum lag in terms of number of records for any partition in this window. " +
                "An increasing value over time is your best indication that the consumer group is not keeping up with the producers.", RECORDS));
        meterTypeMap.put("records-lead", new KafkaMetricMeterType(MeterType.GAUGE, "The latest lead of the partition.", RECORDS));
        meterTypeMap.put("records-lead-min", new KafkaMetricMeterType(MeterType.GAUGE, "The min lead of the partition. The lag between the consumer offset and the start offset " +
                "of the log. If this gets close to zero, it's an indication that the consumer may lose data soon.", RECORDS));
        meterTypeMap.put("records-lead-avg", new KafkaMetricMeterType(MeterType.GAUGE, "The average lead of the partition.", RECORDS));
        meterTypeMap.put("fetch-size-avg", new KafkaMetricMeterType(MeterType.GAUGE, "The average number of bytes fetched per request.", BaseUnits.BYTES));
        meterTypeMap.put("fetch-size-max", new KafkaMetricMeterType(MeterType.GAUGE, "The maximum number of bytes fetched per request.", BaseUnits.BYTES));
        meterTypeMap.put("records-per-request-avg", new KafkaMetricMeterType(MeterType.GAUGE, "The average number of records in each request.", RECORDS));
        meterTypeMap.put("bytes-consumed-total", new KafkaMetricMeterType(MeterType.FUNCTION_COUNTER, "The total number of bytes consumed.", BaseUnits.BYTES));
        meterTypeMap.put("records-consumed-total", new KafkaMetricMeterType(MeterType.FUNCTION_COUNTER, "The total number of records consumed.", RECORDS));
        meterTypeMap.put("fetch-total", new KafkaMetricMeterType(MeterType.FUNCTION_COUNTER, "The number of fetch requests.", REQUESTS));
        meterTypeMap.put("fetch-latency-avg", new KafkaMetricMeterType(MeterType.TIME_GAUGE, "The average time taken for a fetch request."));
        meterTypeMap.put("fetch-latency-max", new KafkaMetricMeterType(MeterType.TIME_GAUGE, "The max time taken for a fetch request."));
        meterTypeMap.put("fetch-throttle-time-avg", new KafkaMetricMeterType(MeterType.TIME_GAUGE, "The average throttle time. When quotas are enabled, the broker may delay fetch " +
                "requests in order to throttle a consumer which has exceeded its limit. This metric indicates how throttling time has been added to fetch requests on average."));
        meterTypeMap.put("fetch-throttle-time-max", new KafkaMetricMeterType(MeterType.TIME_GAUGE, "The maximum throttle time."));
        meterTypeMap.put("assigned-partitions", new KafkaMetricMeterType(MeterType.GAUGE, "The number of partitions currently assigned to this consumer.",  PARTITIONS));
        meterTypeMap.put("commit-rate", new KafkaMetricMeterType(MeterType.GAUGE, "The number of commit calls per second.", "commits"));
        meterTypeMap.put("join-rate", new KafkaMetricMeterType(MeterType.GAUGE, "The number of group joins per second. Group joining is the first phase of the rebalance protocol. " +
                "A large value indicates that the consumer group is unstable and will likely be coupled with increased lag.", JOINS));
        meterTypeMap.put("sync-rate", new KafkaMetricMeterType(MeterType.GAUGE, "The number of group syncs per second. Group synchronization is the second and last phase of the rebalance protocol. " +
                "A large value indicates group instability.", SYNCS));
        meterTypeMap.put("heartbeat-rate", new KafkaMetricMeterType(MeterType.GAUGE, "The average number of heartbeats per second. After a rebalance, the consumer sends heartbeats to " +
                "the coordinator to keep itself active in the group. " +
                "You may see a lower rate than configured if the processing loop is taking more time to handle message batches. Usually this is OK as long as you see no increase in the join rate.", HEARTBEATS));
        meterTypeMap.put("commit-latency-avg", new KafkaMetricMeterType(MeterType.TIME_GAUGE, "The average time taken for a commit request."));
        meterTypeMap.put("commit-latency-max", new KafkaMetricMeterType(MeterType.TIME_GAUGE, "The max time taken for a commit request."));
        meterTypeMap.put("join-time-avg", new KafkaMetricMeterType(MeterType.TIME_GAUGE, "The average time taken for a group rejoin. This value can get as high as the configured session timeout " +
                "for the consumer, but should usually be lower."));
        meterTypeMap.put("join-time-max", new KafkaMetricMeterType(MeterType.TIME_GAUGE, "The max time taken for a group rejoin. This value should not get much higher than the configured session " +
                "timeout for the consumer."));
        meterTypeMap.put("sync-time-avg", new KafkaMetricMeterType(MeterType.TIME_GAUGE, "The average time taken for a group sync."));
        meterTypeMap.put("sync-time-max", new KafkaMetricMeterType(MeterType.TIME_GAUGE, "The max time taken for a group sync."));
        meterTypeMap.put("heartbeat-response-time-max", new KafkaMetricMeterType(MeterType.TIME_GAUGE, "The max time taken to receive a response to a heartbeat request."));
        meterTypeMap.put("last-heartbeat-seconds-ago", new KafkaMetricMeterType(MeterType.TIME_GAUGE, "The time since the last controller heartbeat.", TimeUnit.SECONDS));
        meterTypeMap.put("connection-count", new KafkaMetricMeterType(MeterType.GAUGE, "The current number of active connections.", CONNECTIONS));
        meterTypeMap.put("connection-creation-total", new KafkaMetricMeterType(MeterType.GAUGE, "New connections established.", CONNECTIONS));
        meterTypeMap.put("connection-close-total", new KafkaMetricMeterType(MeterType.GAUGE, "Connections closed.", CONNECTIONS));
        meterTypeMap.put("io-ratio", new KafkaMetricMeterType(MeterType.GAUGE, "The fraction of time the I/O thread spent doing I/O."));
        meterTypeMap.put("io-wait-ratio", new KafkaMetricMeterType(MeterType.GAUGE, "The fraction of time the I/O thread spent waiting."));
        meterTypeMap.put("select-total", new KafkaMetricMeterType(MeterType.GAUGE, "Number of times the I/O layer checked for new I/O to perform."));
        meterTypeMap.put("io-time-ns-avg", new KafkaMetricMeterType(MeterType.TIME_GAUGE, "The average length of time for I/O per select call.", TimeUnit.NANOSECONDS));
        meterTypeMap.put("io-wait-time-ns-avg", new KafkaMetricMeterType(MeterType.TIME_GAUGE, "The average length of time the I/O thread spent waiting for a socket to be ready for reads or writes.",
                TimeUnit.NANOSECONDS));
        meterTypeMap.put("successful-authentication-total", new KafkaMetricMeterType(MeterType.TIME_GAUGE, "The number of successful authentication attempts.",
                 "authentication-attempts"));
        meterTypeMap.put("failed-authentication-total", new KafkaMetricMeterType(MeterType.TIME_GAUGE, "The number of failed authentication attempts.",
                 "authentication-attempts"));
        meterTypeMap.put("network-io-total", new KafkaMetricMeterType(MeterType.GAUGE, "", BaseUnits.BYTES));
        meterTypeMap.put("outgoing-byte-total", new KafkaMetricMeterType(MeterType.GAUGE, "", BaseUnits.BYTES));
        meterTypeMap.put("request-total", new KafkaMetricMeterType(MeterType.GAUGE, "", REQUESTS));
        meterTypeMap.put("response-total", new KafkaMetricMeterType(MeterType.GAUGE, "", RESPONSES));

        meterTypeMap.put("io-waittime-total", new KafkaMetricMeterType(MeterType.TIME_GAUGE, "Time spent on the I/O thread waiting for a socket to be ready for reads or writes.", TimeUnit.NANOSECONDS));
        meterTypeMap.put("io-time-total", new KafkaMetricMeterType(MeterType.TIME_GAUGE, "Time spent in I/O during select calls.", TimeUnit.NANOSECONDS));
    }

    /**
     * Lookup the {@link KafkaMetricMeterType} for a given metric name.  Will default to GAUGE type.
     *
     * @param name The metric name
     * @return {@link KafkaMetricMeterType} for the metric name
     */
    protected KafkaMetricMeterType lookup(final String name) {
        return meterTypeMap.getOrDefault(name, DEFAULT_KAFKA_METRIC_METER_TYPE);
    }
}

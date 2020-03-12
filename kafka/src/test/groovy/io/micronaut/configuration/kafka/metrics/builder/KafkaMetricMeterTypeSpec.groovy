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
package io.micronaut.configuration.kafka.metrics.builder

import spock.lang.Specification

import java.util.concurrent.TimeUnit

class KafkaMetricMeterTypeSpec extends Specification {

    def "should be able to construct with no args"() {
        when:
        KafkaMetricMeterType kafkaMetricMeterType = new KafkaMetricMeterType()

        then:
        kafkaMetricMeterType.meterType == MeterType.GAUGE
        kafkaMetricMeterType.baseUnit == null
        kafkaMetricMeterType.timeUnit == null
        kafkaMetricMeterType.description == null
    }

    def "should be able to construct with args"() {
        when:
        KafkaMetricMeterType kafkaMetricMeterType = new KafkaMetricMeterType(MeterType.TIME_GAUGE, "a", TimeUnit.DAYS, "c")

        then:
        kafkaMetricMeterType.meterType == MeterType.TIME_GAUGE
        kafkaMetricMeterType.baseUnit == "c"
        kafkaMetricMeterType.timeUnit == TimeUnit.DAYS
        kafkaMetricMeterType.description == "a"
    }
}

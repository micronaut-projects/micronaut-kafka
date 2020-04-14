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

class KafkaMetricMeterTypeRegistrySpec extends Specification {
    def "test lookup method"() {
        given:
        def registry = new KafkaMetricMeterTypeRegistry()

        expect:
        registry.lookup("not exists").meterType == MeterType.GAUGE
        registry.lookup("").meterType == MeterType.GAUGE
        registry.lookup(null).meterType == MeterType.GAUGE
        registry.lookup("records-consumed-total").meterType == MeterType.FUNCTION_COUNTER
        registry.lookup("io-waittime-total").meterType == MeterType.TIME_GAUGE
    }
}

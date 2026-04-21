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
package io.micronaut.configuration.kafka

import io.micronaut.core.annotation.TypeHint
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor
import org.apache.kafka.clients.consumer.RangeAssignor
import org.apache.kafka.clients.consumer.RoundRobinAssignor
import org.apache.kafka.clients.consumer.StickyAssignor
import spock.lang.Specification

class KafkaConsumerFactoryTypeHintSpec extends Specification {

    void 'registers kafka partition assignors for native image reflection'() {
        given:
        TypeHint typeHint = KafkaConsumerFactory.getAnnotation(TypeHint)

        expect:
        typeHint != null
        typeHint.value().toSet() == [
            RangeAssignor,
            CooperativeStickyAssignor,
            RoundRobinAssignor,
            StickyAssignor
        ] as Set
        typeHint.accessType().contains(TypeHint.AccessType.ALL_PUBLIC_CONSTRUCTORS)
    }
}

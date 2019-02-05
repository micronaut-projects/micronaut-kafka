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
package io.micronaut.configuration.kafka.health

import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration
import io.micronaut.context.ApplicationContext
import io.micronaut.health.HealthStatus
import io.micronaut.management.health.indicator.HealthResult
import spock.lang.Specification

class KafkaHealthIndicatorSpec extends Specification {

    void "test kafka health indicator"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                Collections.singletonMap(
                        AbstractKafkaConfiguration.EMBEDDED, true
                )
        )

        when:
        KafkaHealthIndicator healthIndicator = applicationContext.getBean(KafkaHealthIndicator)
        HealthResult result = healthIndicator.result.firstElement().blockingGet()

        then:
        // report down because the not enough nodes to meet replication factor
        result.status == HealthStatus.DOWN
        result.details.nodes == 1

        cleanup:
        applicationContext.close()
    }
}

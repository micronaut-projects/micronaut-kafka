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
package io.micronaut.configuration.kafka.streams.health

import io.micronaut.configuration.kafka.streams.AbstractTestContainersSpec
import io.micronaut.configuration.kafka.streams.KafkaStreamsFactory
import io.micronaut.health.HealthStatus
import io.micronaut.management.health.aggregator.HealthAggregator
import io.micronaut.management.health.aggregator.RxJavaHealthAggregator
import io.micronaut.management.health.indicator.HealthResult
import io.micronaut.runtime.ApplicationConfiguration
import io.reactivex.Single

class KafkaStreamsHealthSpec extends AbstractTestContainersSpec {

    def "should create object"() {
        given:
        def kafkaStreamsFactory = embeddedServer.getApplicationContext().getBean(KafkaStreamsFactory)
        HealthAggregator healthAggregator = new RxJavaHealthAggregator(Mock(ApplicationConfiguration))

        when:
        KafkaStreamsHealth kafkaStreamsHealth = new KafkaStreamsHealth(kafkaStreamsFactory, healthAggregator)

        then:
        kafkaStreamsHealth
    }

    def "should check health"() {
        given:
        def streamsHealth = embeddedServer.getApplicationContext().getBean(KafkaStreamsHealth)

        expect:
        conditions.eventually {
            Single.fromPublisher(streamsHealth.getResult()).blockingGet().status == HealthStatus.UP
        }

        and:
        def healthLevelOne = Single.fromPublisher(streamsHealth.getResult()).blockingGet()
        assert ((HashMap<String, HealthResult>) healthLevelOne.details).find { it.key == "micronaut-kafka-streams" }
        HealthResult healthLevelTwo = ((HashMap<String, HealthResult>) healthLevelOne.details).find { it.key == "micronaut-kafka-streams" }?.value
        healthLevelTwo.name == "micronaut-kafka-streams"
        healthLevelTwo.status == HealthStatus.UP
        (healthLevelTwo.details as Map).size() == 8
        (healthLevelTwo.details as Map).containsKey("adminClientId")
        (healthLevelTwo.details as Map).containsKey("restoreConsumerClientId")
        (healthLevelTwo.details as Map).containsKey("threadState")
        (healthLevelTwo.details as Map).containsKey("producerClientIds")
        (healthLevelTwo.details as Map).containsKey("standbyTasks")
        (healthLevelTwo.details as Map).containsKey("activeTasks")
        (healthLevelTwo.details as Map).containsKey("consumerClientId")
        (healthLevelTwo.details as Map).containsKey("threadName")
    }
}

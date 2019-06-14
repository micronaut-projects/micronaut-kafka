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
package io.micronaut.configuration.kafka.embedded

import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration
import io.micronaut.configuration.kafka.config.AbstractKafkaConsumerConfiguration
import io.micronaut.context.ApplicationContext
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import spock.lang.Specification

class KafkaEmbeddedSpec extends Specification {

    void "test run kafka embedded server"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                Collections.singletonMap(
                        AbstractKafkaConfiguration.EMBEDDED, true
                )
        )

        when:
        AbstractKafkaConsumerConfiguration config = applicationContext.getBean(AbstractKafkaConsumerConfiguration)
        Properties props = config.getConfig()

        then:
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] == AbstractKafkaConfiguration.DEFAULT_BOOTSTRAP_SERVERS


        when:
        KafkaEmbedded kafkaEmbedded = applicationContext.getBean(KafkaEmbedded)

        then:
        kafkaEmbedded.kafkaServer.isPresent()
        kafkaEmbedded.zkPort.isPresent()

        cleanup:
        applicationContext.close()
    }

    void "test run kafka embedded server with multiple partitions topic"() {
        given:
        int partitionNumber = 10
        String topicName = "my-topic"

        ApplicationContext applicationContext = ApplicationContext.run(
                new HashMap() {
                    {
                        put(AbstractKafkaConfiguration.EMBEDDED, true)
                        put(AbstractKafkaConfiguration.EMBEDDED_TOPICS, topicName)
                        put("kafka.embedded.properties.num.partitions", partitionNumber)
                    }
                }
        )

        AdminClient adminClient = createAdminClient()

        when:
        KafkaEmbedded kafkaEmbedded = applicationContext.getBean(KafkaEmbedded)

        then:
        kafkaEmbedded.kafkaServer.isPresent()
        kafkaEmbedded.zkPort.isPresent()

        and:
        adminClient
                .describeTopics([topicName]).values()
                .get(topicName).get()
                .partitions().size() == partitionNumber

        cleanup:
        adminClient.close()
        applicationContext.close()
    }

    private static AdminClient createAdminClient() {
        Properties properties = new Properties()
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, AbstractKafkaConfiguration.DEFAULT_BOOTSTRAP_SERVERS)
        AdminClient adminClient = AdminClient.create(properties)
        adminClient
    }
}

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
package io.micronaut.configuration.kafka

import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration
import io.micronaut.configuration.kafka.config.AbstractKafkaConsumerConfiguration
import io.micronaut.configuration.kafka.config.KafkaConsumerConfiguration
import io.micronaut.context.ApplicationContext
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import spock.lang.Specification

class KafkaConfigurationSpec extends Specification {



    void "test default consumer configuration"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                ("kafka." + ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name,
                ("kafka." + ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name
        )

        when:
        AbstractKafkaConsumerConfiguration config = applicationContext.getBean(AbstractKafkaConsumerConfiguration)
        Properties props = config.getConfig()

        then:
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] == AbstractKafkaConfiguration.DEFAULT_BOOTSTRAP_SERVERS

        when:
        Consumer consumer = applicationContext.createBean(Consumer, config)

        then:
        consumer != null

        cleanup:
        consumer.close()
        applicationContext.close()
    }



    void "test configure default properties"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                ("kafka.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}".toString()):"localhost:1111",
                ("kafka.${ConsumerConfig.GROUP_ID_CONFIG}".toString()):"mygroup",
                ("kafka.${ConsumerConfig.MAX_POLL_RECORDS_CONFIG}".toString()):"100",
                ("kafka." + ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name,
                ("kafka." + ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name

        )

        when:
        AbstractKafkaConsumerConfiguration config = applicationContext.getBean(AbstractKafkaConsumerConfiguration)
        Properties props = config.getConfig()

        then:
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] == "localhost:1111"
        props[ConsumerConfig.GROUP_ID_CONFIG] == "mygroup"
        props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] == "100"

        when:
        Consumer consumer = applicationContext.createBean(Consumer, config)

        then:
        consumer != null

        cleanup:
        consumer.close()
        applicationContext.close()
    }

    void "test override consumer default properties"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                ("kafka.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}".toString()):"localhost:1111",
                ("kafka.${ConsumerConfig.GROUP_ID_CONFIG}".toString()):"mygroup",
                ("kafka.consumers.default.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}".toString()):"localhost:2222",
                ("kafka.${ConsumerConfig.GROUP_ID_CONFIG}".toString()):"mygroup",
                ("kafka.consumers.default.${ConsumerConfig.MAX_POLL_RECORDS_CONFIG}".toString()):"100",
                ("kafka.consumers.default." + ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name,
                ("kafka.consumers.default." + ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name

        )

        when:
        KafkaConsumerConfiguration config = applicationContext.getBean(KafkaConsumerConfiguration)
        Properties props = config.getConfig()

        then:
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] == "localhost:2222"
        props[ConsumerConfig.GROUP_ID_CONFIG] == "mygroup"
        props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] == "100"

        when:
        Consumer consumer = applicationContext.createBean(Consumer, config)

        then:
        consumer != null

        cleanup:
        consumer.close()
        applicationContext.close()
    }
}

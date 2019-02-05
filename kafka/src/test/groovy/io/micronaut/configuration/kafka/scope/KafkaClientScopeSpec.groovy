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
package io.micronaut.configuration.kafka.scope

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.context.ApplicationContext
import org.apache.kafka.clients.producer.KafkaProducer
import spock.lang.Specification

import javax.inject.Inject
import javax.inject.Singleton

class KafkaClientScopeSpec extends Specification {

    void "test inject kafka producer"() {
        given:
        ApplicationContext ctx = ApplicationContext.run(
                "kafka.producers.foo.acks":"all"
        )

        when:
        MyClass myClass = ctx.getBean(MyClass)

        then:
        myClass.producer != null
        myClass.producer.@producerConfig.getString("acks") == "all"

        cleanup:
        ctx.close()
    }


    @Singleton
    static class MyClass {
        @Inject @KafkaClient("foo") KafkaProducer<String, Integer> producer
    }
}

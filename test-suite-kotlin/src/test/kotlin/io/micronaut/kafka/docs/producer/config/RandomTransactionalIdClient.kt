package io.micronaut.kafka.docs.producer.config

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.context.annotation.Prototype
import io.micronaut.context.annotation.Requires

@Requires(property = "spec.name", value = "RandomTransactionalIdClientTest")
// tag::clazz[]
@Prototype
@KafkaClient(id = "my-client", transactionalId = "my-tx-id-\${random.uuid}")
interface RandomTransactionalIdClient {
    // define client API
}
// end::clazz[]

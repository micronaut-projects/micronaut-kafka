package io.micronaut.kafka.docs.quickstart

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import org.slf4j.LoggerFactory
// end::imports[]

@Requires(property = "spec.name", value = "QuickstartTest")
// tag::clazz[]
@KafkaListener(offsetReset = OffsetReset.EARLIEST) // <1>
class ProductListener {
    companion object {
        private val LOG = LoggerFactory.getLogger(ProductListener::class.java)
    }

    @Topic("my-products") // <2>
    fun receive(@KafkaKey brand: String?, name: String?) { // <3>
        LOG.info("Got Product - {} by {}", name, brand)
    }
}

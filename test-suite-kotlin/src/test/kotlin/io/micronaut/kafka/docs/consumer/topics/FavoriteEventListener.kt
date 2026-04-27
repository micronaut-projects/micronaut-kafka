package io.micronaut.kafka.docs.consumer.topics

import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires

@Requires(property = "spec.name", value = "FavoriteEventListenerTest")
@KafkaListener
class FavoriteEventListener {

    // tag::commonSupertype[]
    @Topic("favorites-events")
    fun receive(@KafkaKey customerId: String, event: FavoriteEvent) {
        when (event) {
            is FavoriteSaved -> handleSaved(customerId, event)
            is FavoriteDeleted -> handleDeleted(customerId, event)
        }
    }
    // end::commonSupertype[]

    private fun handleSaved(customerId: String, favoriteSaved: FavoriteSaved) {
    }

    private fun handleDeleted(customerId: String, favoriteDeleted: FavoriteDeleted) {
    }
}

sealed interface FavoriteEvent

class FavoriteSaved : FavoriteEvent

class FavoriteDeleted : FavoriteEvent

package io.micronaut.kafka.docs.consumer.topics

import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires

@Requires(property = 'spec.name', value = 'FavoriteEventListenerTest')
@KafkaListener
class FavoriteEventListener {

    // tag::commonSupertype[]
    @Topic('favorites-events')
    void receive(@KafkaKey String customerId, FavoriteEvent event) {
        if (event instanceof FavoriteSaved) {
            handleSaved(customerId, event)
        } else if (event instanceof FavoriteDeleted) {
            handleDeleted(customerId, event)
        }
    }
    // end::commonSupertype[]

    private void handleSaved(String customerId, FavoriteSaved favoriteSaved) {
    }

    private void handleDeleted(String customerId, FavoriteDeleted favoriteDeleted) {
    }

    static abstract class FavoriteEvent {
    }

    static final class FavoriteSaved extends FavoriteEvent {
    }

    static final class FavoriteDeleted extends FavoriteEvent {
    }
}

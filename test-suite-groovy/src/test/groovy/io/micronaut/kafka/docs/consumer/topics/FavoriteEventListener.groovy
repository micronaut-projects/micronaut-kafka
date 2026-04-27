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
            // process the save event for this customer
        } else if (event instanceof FavoriteDeleted) {
            // process the delete event for this customer
        }
    }
    // end::commonSupertype[]

    static abstract class FavoriteEvent {
    }

    static final class FavoriteSaved extends FavoriteEvent {
    }

    static final class FavoriteDeleted extends FavoriteEvent {
    }
}

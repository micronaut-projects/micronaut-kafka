from abc import ABC
from typing import Annotated

from micronaut.configuration.kafka.annotation import KafkaKey, KafkaListener, Topic
from micronaut.context.annotation import Requires


class FavoriteEvent(ABC):
    pass


class FavoriteSaved(FavoriteEvent):
    pass


class FavoriteDeleted(FavoriteEvent):
    pass


@Requires(property="spec.name", value="FavoriteEventListenerTest")
@KafkaListener
class FavoriteEventListener:

    # tag::commonSupertype[]
    @Topic("favorites-events")
    def receive(self, customer_id: Annotated[str, KafkaKey], event: FavoriteEvent) -> None:
        if isinstance(event, FavoriteSaved):
            # process the save event for this customer
            pass
        elif isinstance(event, FavoriteDeleted):
            # process the delete event for this customer
            pass
    # end::commonSupertype[]

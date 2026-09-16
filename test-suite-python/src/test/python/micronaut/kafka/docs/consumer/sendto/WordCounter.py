import re
from collections import Counter

from micronaut.configuration.kafka import KafkaMessage
from micronaut.configuration.kafka.annotation import KafkaListener, OffsetReset, OffsetStrategy, Topic
from micronaut.context.annotation import Requires
from micronaut.messaging.annotation import SendTo
from org.apache.kafka.common import IsolationLevel
from org.apache.kafka.common.utils import Utils


@Requires(property="spec.name", value="WordCounterTest")
# tag::transactional[]
@KafkaListener(
    offsetReset=OffsetReset.EARLIEST,
    producerClientId="word-counter-producer",  # <1>
    producerTransactionalId="tx-word-counter-id",  # <2>
    offsetStrategy=OffsetStrategy.SEND_TO_TRANSACTION,  # <3>
    isolation=IsolationLevel.READ_COMMITTED  # <4>
)
class WordCounter:

    @Topic("tx-incoming-strings")
    @SendTo("my-words-count")
    def words_counter(self, string: str) -> list[KafkaMessage[bytes, int]]:
        return [KafkaMessage.Builder.withBody(count).key(Utils.utf8(word)).build()
                for word, count in Counter(re.split(r"\s+", string)).items()]
# end::transactional[]

import logging
from typing import Annotated

from micronaut.configuration.kafka.annotation import KafkaKey, KafkaListener, OffsetReset, Topic
from micronaut.context.annotation import Requires
from org.apache.kafka.common.utils import Utils

LOG = logging.getLogger(__name__)


@Requires(property="spec.name", value="WordCounterTest")
@KafkaListener(offsetReset=OffsetReset.EARLIEST)
class WordCountListener:

    def __init__(self):
        self.word_count: dict[str, int] = {}

    @Topic("my-words-count")
    def receive(self, key: Annotated[bytes, KafkaKey], value: object) -> None:
        word = Utils.utf8(key)
        count = int(value)
        LOG.info("Got word count - %s: %s", word, count)
        self.word_count[word] = count

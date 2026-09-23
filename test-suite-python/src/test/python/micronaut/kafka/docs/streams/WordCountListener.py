# tag::imports[]
from typing import Annotated

from java.lang import Long
from micronaut.configuration.kafka.annotation import KafkaKey, KafkaListener, OffsetReset, Topic
from micronaut.context.annotation import Requires
# end::imports[]


@Requires(property="spec.name", value="WordCountStreamTest")
# tag::clazz[]
@KafkaListener(offsetReset=OffsetReset.EARLIEST, groupId="WordCountListener")
class WordCountListener:

    def __init__(self):
        self.word_counts: dict[str, int] = {}

    @Topic("streams-wordcount-output")
    def count(self, word: Annotated[str, KafkaKey], count: Long) -> None:
        self.word_counts[word] = int(count)

    def get_count(self, word: str) -> int:
        return self.word_counts.get(word, 0)

    def get_word_counts(self) -> dict[str, int]:
        return dict(self.word_counts)
# end::clazz[]

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
package io.micronaut.configuration.kafka.streams.wordcount;

import io.micronaut.configuration.kafka.annotation.*;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@KafkaListener(offsetReset = OffsetReset.EARLIEST)
public class WordCountListener {

    private final Map<String, Long> wordCounts = new ConcurrentHashMap<>();

    @Topic(WordCountStream.OUTPUT)
    void count(@KafkaKey String word, long count) {
        System.out.println("word = " + word);
        wordCounts.put(word, count);
    }

    public long getCount(String word) {
        Long num = wordCounts.get(word);
        if (num != null) {
            return num;
        }
        return 0;
    }

    public Map<String, Long> getWordCounts() {
        return Collections.unmodifiableMap(wordCounts);
    }
}

package io.micronaut.configuration.kafka.streams.listeners;

import io.micronaut.configuration.kafka.streams.event.BeforeKafkaStreamStart;
import io.micronaut.runtime.event.annotation.EventListener;

import javax.inject.Singleton;

@Singleton
public class BeforeStartKafkaStreamsListenerImp  {
    private boolean executed = false;

    @EventListener
    public void execute(BeforeKafkaStreamStart event) {
        executed = true;
    }

    public boolean isExecuted() {
        return executed;
    }
}

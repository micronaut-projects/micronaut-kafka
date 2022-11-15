package io.micronaut.configuration.kafka.streams.metrics;

import io.micronaut.configuration.kafka.metrics.AbstractKafkaMetricsReporter;

import javax.annotation.PreDestroy;

public class KafkaStreamsMetricsReporter extends AbstractKafkaMetricsReporter {
    @Override
    protected String getMetricPrefix() {
        return "kafka-streams";
    }

    /**
     * Method to close bean.  This must remain here for metrics to continue to function correctly for some reason?!.
     */
    @PreDestroy
    @Override
    public void close() {
        super.close();
    }
}

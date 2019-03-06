package io.micronaut.configuration.kafka.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.lang.NonNull;
import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration;
import io.micronaut.core.annotation.Internal;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import javax.annotation.PreDestroy;
import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * A {@link MetricsReporter} that binds metrics to micrometer.
 */
@Internal
public class KafkaMetricsReporter implements MetricsReporter, MeterBinder, Closeable {

    private static final Collection<MeterRegistry> METER_REGISTRIES = new ConcurrentLinkedQueue<>();

    private List<KafkaMetric> metrics;

    @Override
    public void bindTo(@NonNull MeterRegistry registry) {
        METER_REGISTRIES.add(registry);
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        this.metrics = metrics;
        for (MeterRegistry meterRegistry : METER_REGISTRIES) {
            for (KafkaMetric metric : metrics) {
                registerMetric(meterRegistry, metric);
            }
        }
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        for (MeterRegistry meterRegistry : METER_REGISTRIES) {
            registerMetric(meterRegistry, metric);
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        // no-op (Micrometer doesn't support removal)
    }

    @PreDestroy
    @Override
    public void close() {
        if (metrics != null) {
            metrics.clear();
            metrics = null;
        }
        METER_REGISTRIES.clear();
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    private void registerMetric(MeterRegistry meterRegistry, KafkaMetric metric) {
        MetricName metricName = metric.metricName();
        Object v = metric.metricValue();
        if (v instanceof Double) {
            List<Tag> tags = metricName
                    .tags()
                    .entrySet()
                    .stream()
                    .map(entry -> Tag.of(entry.getKey(), entry.getValue()))
                    .collect(Collectors.toList());
            String name = AbstractKafkaConfiguration.PREFIX + '.' + metricName.name();
            meterRegistry.gauge(name, tags, metric, value -> (Double) value.metricValue());
        }
    }
}

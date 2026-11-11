package io.micronaut.configuration.kafka.metrics

import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.composite.CompositeMeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.JmxReporter
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.metrics.Measurable
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.Time
import org.slf4j.LoggerFactory
import spock.lang.Requires
import spock.lang.Shared
import spock.lang.Specification

import java.lang.management.ManagementFactory
import java.lang.management.ThreadMXBean
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration

/**
 * Measures what it costs to register Kafka client metrics with Micrometer through the Micronaut reporter, and pins
 * the re-registration behaviour of {@link io.micronaut.configuration.kafka.metrics.builder.KafkaMetricMeterTypeBuilder}.
 *
 * The cost test creates real {@link KafkaConsumer}s without a broker: their constructors register their metrics but
 * never connect. The registry mirrors a typical Micronaut runtime without Prometheus: a {@link CompositeMeterRegistry}
 * with one {@link SimpleMeterRegistry} child. It prints one row per scenario and writes them to
 * build/registration-cost/latest.tsv.
 *
 * The cost test is a measurement, not a check, so it only runs when the MICRONAUT_KAFKA_REGISTRATION_COST
 * environment variable is set:
 *
 * MICRONAUT_KAFKA_REGISTRATION_COST=true ./gradlew :micronaut-kafka:test --rerun --tests '*KafkaMetricsRegistrationCostSpec'
 */
class KafkaMetricsRegistrationCostSpec extends Specification {

    private static final List<Integer> CONSUMER_COUNTS = [10, 20, 40, 80]
    private static final int WARMUP_ROUNDS = 2
    private static final int MEASURED_ROUNDS = 5
    private static final String METER_PREFIX = "kafka."

    @Shared
    ThreadMXBean threadMx = ManagementFactory.threadMXBean

    @Shared
    Object kafkaLogger

    @Shared
    Object kafkaLoggerLevel

    void setupSpec() {
        // Kafka logs its full config for every consumer; keep that noise out of the timings (same for every variant)
        def logger = LoggerFactory.getLogger("org.apache.kafka")
        if (logger.class.name == "ch.qos.logback.classic.Logger") {
            kafkaLogger = logger
            kafkaLoggerLevel = logger.level
            logger.level = Class.forName("ch.qos.logback.classic.Level").getField("WARN").get(null)
        }
    }

    void cleanupSpec() {
        if (kafkaLogger != null) {
            kafkaLogger.level = kafkaLoggerLevel
        }
    }

    @Requires({ env.MICRONAUT_KAFKA_REGISTRATION_COST })
    void "registration cost of Kafka consumer metrics, by number of consumers"() {
        given:
        def variants = [
                micronaut: ConsumerKafkaMetricsReporter.name,  // the binder
                jmx      : JmxReporter.name,                   // Kafka's default when the binder is off
                none     : "",                                 // no reporter at all: the floor
        ]
        def rows = []
        def header = "variant\tconsumers\tkafka_meters\twall_ms_median\tcpu_ms_median\talloc_mb_median\twall_ms_rounds"

        when:
        variants.each { variant, reporterClass ->
            CONSUMER_COUNTS.each { int consumers ->
                List<Map> rounds = []
                (WARMUP_ROUNDS + MEASURED_ROUNDS).times { int round ->
                    def result = measureRound(consumers, reporterClass)
                    if (round >= WARMUP_ROUNDS) {
                        rounds << result
                    }
                }
                def meters = rounds*.kafkaMeters.unique()
                assert meters.size() == 1: "meter count should be the same in every round: ${meters}"
                rows << [variant, consumers, meters[0],
                         fmt(median(rounds*.wallMs)), fmt(median(rounds*.cpuMs)), fmt(median(rounds*.allocMb)),
                         rounds*.wallMs.collect { fmt(it) }.join(",")].join("\t")
            }
        }
        def out = Path.of("build", "registration-cost", "latest.tsv")
        Files.createDirectories(out.parent)
        Files.writeString(out, ([header] + rows).join("\n") + "\n")
        println(([header] + rows).collect { "REGCOST " + it }.join("\n"))

        then:
        rows.size() == variants.size() * CONSUMER_COUNTS.size()
        rows.findAll { it.startsWith("micronaut") }.every { (it.split("\t")[2] as int) > 0 }
    }

    void "re-registering a metric points the gauge at the new Kafka metric"() {
        given:
        def registry = compositeRegistry()
        def reporter = new ConsumerKafkaMetricsReporter()
        reporter.bindTo(registry)
        def tags = [(AbstractKafkaMetricsReporter.CLIENT_ID_TAG): "consumer-1"]
        def first = fixedValueMetric("records-lag-max", "consumer-fetch-manager-metrics", tags, 1d)
        def second = fixedValueMetric("records-lag-max", "consumer-fetch-manager-metrics", tags, 2d)

        when:
        reporter.metricChange(first)
        reporter.metricChange(second)
        def meters = registry.find("kafka.consumer.fetch.manager.records.lag.max").meters()

        then:
        meters.size() == 1
        meters[0].measure()[0].value == 2d
        // Gauges only hold weak references to the Kafka metric they read; keep both reachable until here
        [first, second].every { it != null }

        cleanup:
        reporter.close()
        registry.close()
    }

    void "re-registering a client-level metric keeps the topic-level meter with the same name"() {
        given:
        def registry = compositeRegistry()
        def reporter = new ConsumerKafkaMetricsReporter()
        reporter.bindTo(registry)
        def clientTags = [(AbstractKafkaMetricsReporter.CLIENT_ID_TAG): "consumer-1"]
        def topicTags = clientTags + [(AbstractKafkaMetricsReporter.TOPIC_TAG): "topic-1"]
        def group = "consumer-fetch-manager-metrics"

        // Gauges only hold weak references to the Kafka metric they read, so keep these reachable (Kafka's Metrics does)
        def metrics = [
                fixedValueMetric("bytes-consumed-rate", group, clientTags, 1d),
                fixedValueMetric("bytes-consumed-rate", group, topicTags, 10d),
                fixedValueMetric("bytes-consumed-rate", group, clientTags, 2d),
        ]

        when:
        metrics.each { reporter.metricChange(it) }
        def byTopic = registry.find("kafka.consumer.fetch.manager.bytes.consumed.rate").meters()
                .collectEntries { Meter m -> [(m.id.getTag("topic") ?: "<client>"): m.measure()[0].value] }

        then:
        byTopic == ["<client>": 2d, "topic-1": 10d]
        metrics.size() == 3

        cleanup:
        reporter.close()
        registry.close()
    }

    private Map measureRound(int consumers, String reporterClass) {
        def registry = compositeRegistry()
        List<KafkaConsumer> created = []
        System.gc()
        long cpu0 = cpuNanos()
        long alloc0 = allocatedBytes()
        long t0 = System.nanoTime()
        consumers.times { int i ->
            created << new KafkaConsumer<String, String>(consumerConfig(i, reporterClass, registry))
        }
        long t1 = System.nanoTime()
        long cpu1 = cpuNanos()
        long alloc1 = allocatedBytes()
        int kafkaMeters = registry.meters.count { it.id.name.startsWith(METER_PREFIX) } as int
        created.each { it.close(Duration.ZERO) }
        registry.close()
        [wallMs: (t1 - t0) / 1e6d, cpuMs: delta(cpu0, cpu1, 1e6d), allocMb: delta(alloc0, alloc1, 1048576d), kafkaMeters: kafkaMeters]
    }

    // CPU time and allocated bytes are optional JVM features: report NaN where they aren't available
    private long cpuNanos() {
        threadMx.currentThreadCpuTimeSupported ? threadMx.currentThreadCpuTime : -1L
    }

    private long allocatedBytes() {
        threadMx instanceof com.sun.management.ThreadMXBean && threadMx.threadAllocatedMemorySupported && threadMx.threadAllocatedMemoryEnabled ?
                threadMx.currentThreadAllocatedBytes : -1L
    }

    private static double delta(long before, long after, double unit) {
        before < 0 || after < 0 ? Double.NaN : (after - before) / unit
    }

    private static Map<String, Object> consumerConfig(int i, String reporterClass, MeterRegistry registry) {
        [
                (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)              : "localhost:9092",
                (ConsumerConfig.GROUP_ID_CONFIG)                       : "registration-cost-group-${i}".toString(),
                (ConsumerConfig.CLIENT_ID_CONFIG)                      : "registration-cost-consumer-${i}".toString(),
                (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)         : StringDeserializer,
                (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)       : StringDeserializer,
                (CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG)   : reporterClass,
                "meter.registry"                                       : registry,
        ]
    }

    private static CompositeMeterRegistry compositeRegistry() {
        def registry = new CompositeMeterRegistry()
        registry.add(new SimpleMeterRegistry())
        registry
    }

    private static KafkaMetric fixedValueMetric(String name, String group, Map<String, String> tags, double value) {
        new KafkaMetric(new Object(), new MetricName(name, group, "description", tags),
                { MetricConfig config, long now -> value } as Measurable, new MetricConfig(), Time.SYSTEM)
    }

    private static double median(List<Double> values) {
        def sorted = values.sort(false)
        sorted.size() % 2 == 1 ? sorted[sorted.size().intdiv(2)] :
                (sorted[sorted.size().intdiv(2) - 1] + sorted[sorted.size().intdiv(2)]) / 2d
    }

    private static String fmt(double d) {
        String.format(Locale.ROOT, "%.1f", d)
    }
}

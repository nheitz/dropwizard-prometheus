package org.dhatim.dropwizard.prometheus;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class DropwizardMetricsExporter {

    private static final Logger LOG = LoggerFactory.getLogger(DropwizardMetricsExporter.class);

    private final PrometheusTextWriter writer;

    public DropwizardMetricsExporter(PrometheusTextWriter writer) {
        this.writer = writer;
    }

    public void writeGauge(String name, Gauge<?> gauge, Map<String, String> labels) throws IOException {
        final String sanitizedName = sanitizeMetricName(name);
        writer.writeHelp(sanitizedName, getHelpMessage(name, gauge));
        writer.writeType(sanitizedName, MetricType.GAUGE);

        Object obj = gauge.getValue();
        double value;
        if (obj instanceof Number) {
            value = ((Number) obj).doubleValue();
        } else if (obj instanceof Boolean) {
            value = ((Boolean) obj) ? 1 : 0;
        } else {
            LOG.trace("Invalid type for Gauge {}: {}", name, obj.getClass().getName());
            return;
        }

        writer.writeSample(sanitizedName, labels, value);
    }

    /**
     * Export counter as Prometheus <a href="https://prometheus.io/docs/concepts/metric_types/#gauge">Gauge</a>.
     */
    public void writeCounter(String dropwizardName, Counter counter, Map<String, String> labels) throws IOException {
        String name = sanitizeMetricName(dropwizardName);
        writer.writeHelp(name, getHelpMessage(dropwizardName, counter));
        writer.writeType(name, MetricType.GAUGE);
        writer.writeSample(name, labels, counter.getCount());
    }


    /**
     * Export a histogram snapshot as a prometheus SUMMARY.
     *
     * @param dropwizardName metric name.
     * @throws IOException
     */
    public void writeHistogram(String dropwizardName, Histogram histogram, Map<String, String> labels) throws IOException {
        writeSnapshotAndCount(dropwizardName, histogram.getSnapshot(), histogram.getCount(), 1.0, MetricType.SUMMARY,
                getHelpMessage(dropwizardName, histogram), labels);
    }


    private void writeSnapshotAndCount(String dropwizardName, Snapshot snapshot, long count, double factor,
                                       MetricType type, String helpMessage, Map<String, String> labels) throws IOException {
        String name = sanitizeMetricName(dropwizardName);
        Map<String, String> labelsCopy = new HashMap<>(labels);
        String quantileName = name + "_quantile";

        writer.writeHelp(name, helpMessage);
        writer.writeType(name, type);
        writer.writeSample(quantileName, mapOf("quantile", "0.5", labelsCopy), snapshot.getMedian() * factor);
        writer.writeSample(quantileName, mapOf("quantile", "0.75", labelsCopy), snapshot.get75thPercentile() * factor);
        writer.writeSample(quantileName, mapOf("quantile", "0.95", labelsCopy), snapshot.get95thPercentile() * factor);
        writer.writeSample(quantileName, mapOf("quantile", "0.98", labelsCopy), snapshot.get98thPercentile() * factor);
        writer.writeSample(quantileName, mapOf("quantile", "0.99", labelsCopy), snapshot.get99thPercentile() * factor);
        writer.writeSample(quantileName, mapOf("quantile", "0.999", labelsCopy), snapshot.get999thPercentile() * factor);
        writer.writeSample(name + "_min", labelsCopy, snapshot.getMin());
        writer.writeSample(name + "_max", labelsCopy, snapshot.getMax());
        writer.writeSample(name + "_median", labelsCopy, snapshot.getMedian());
        writer.writeSample(name + "_mean", labelsCopy, snapshot.getMean());
        writer.writeSample(name + "_stddev", labelsCopy, snapshot.getStdDev());
        writer.writeSample(name + "_count", labelsCopy, count);
    }

    private void writeMetered(String dropwizardName, Metered metered, Map<String, String> labels) throws IOException {
        String name = sanitizeMetricName(dropwizardName);
        Map<String, String> labelsCopy = new HashMap<>(labels);
        String rateName = name + "_rate";

        writer.writeSample(rateName, mapOf("rate", "m1", labelsCopy), metered.getOneMinuteRate());
        writer.writeSample(rateName, mapOf("rate", "m5", labelsCopy), metered.getFiveMinuteRate());
        writer.writeSample(rateName, mapOf("rate", "m15", labelsCopy), metered.getFifteenMinuteRate());
        writer.writeSample(rateName, mapOf("rate", "mean", labelsCopy), metered.getMeanRate());
    }

    private Map<String, String> mapOf(String key, String value, Map<String, String> mapTo) {
        mapTo.put(key, value);
        return mapTo;
    }

    private Map<String, String> emptyMap() {
        return Collections.emptyMap();
    }

    public void writeTimer(String dropwizardName, Timer timer, Map<String, String> labels) throws IOException {
        writeSnapshotAndCount(dropwizardName, timer.getSnapshot(), timer.getCount(),
                1.0D / TimeUnit.SECONDS.toNanos(1L), MetricType.SUMMARY, getHelpMessage(dropwizardName, timer), labels);
        writeMetered(dropwizardName, timer, labels);
    }

    public void writeMeter(String dropwizardName, Meter meter, Map<String, String> labels) throws IOException {
        String name = sanitizeMetricName(dropwizardName) + "_total";

        writer.writeHelp(name, getHelpMessage(dropwizardName, meter));
        writer.writeType(name, MetricType.COUNTER);
        writer.writeSample(name, labels, meter.getCount());

        writeMetered(dropwizardName, meter, labels);
    }

    private static String getHelpMessage(String metricName, Metric metric) {
        return String.format("Generated from Dropwizard metric import (metric=%s, type=%s)", metricName,
                metric.getClass().getName());
    }

    static String sanitizeMetricName(String dropwizardName) {
        return dropwizardName.replaceAll("[^a-zA-Z0-9:_]", "_");
    }

}

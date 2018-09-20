package org.dhatim.dropwizard.prometheus;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.servlets.MetricsServlet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.emptyMap;

@SuppressWarnings("serial")
class PrometheusServlet extends HttpServlet {

    public static final String METRICS_REGISTRY = MetricsServlet.class.getCanonicalName() + ".registry";
    public static final String METRIC_FILTER = MetricsServlet.class.getCanonicalName() + ".metricFilter";
    public static final String ALLOWED_ORIGIN = MetricsServlet.class.getCanonicalName() + ".allowedOrigin";

    public static final Pattern LABEL_PATTERN = Pattern.compile("\\{([^\\}]+)\\}");
    public static final Pattern MULTIDOT_PATTERN = Pattern.compile("\\.{2,}");
    public static final Pattern KV_PAIR_PATTERN = Pattern.compile("(.+):(.+)");

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusServlet.class);

    private MetricRegistry registry;

    private String allowedOrigin;

    private MetricFilter filter;

    public PrometheusServlet(MetricRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);

        final ServletContext context = config.getServletContext();
        if (null == registry) {
            final Object registryAttr = context.getAttribute(METRICS_REGISTRY);
            if (registryAttr instanceof MetricRegistry) {
                this.registry = (MetricRegistry) registryAttr;
            } else {
                throw new ServletException("Couldn't find a MetricRegistry instance.");
            }
        }

        filter = (MetricFilter) context.getAttribute(METRIC_FILTER);
        if (filter == null) {
            filter = MetricFilter.ALL;
        }

        this.allowedOrigin = context.getInitParameter(ALLOWED_ORIGIN);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType(TextFormat.CONTENT_TYPE);
        if (allowedOrigin != null) {
            resp.setHeader("Access-Control-Allow-Origin", allowedOrigin);
        }
        resp.setHeader("Cache-Control", "must-revalidate,no-cache,no-store");
        resp.setStatus(HttpServletResponse.SC_OK);

        Set<String> filtered = parse(req);

        PrometheusTextWriter writer = new PrometheusTextWriter(resp.getWriter());
        try {
            DropwizardMetricsExporter exporter = new DropwizardMetricsExporter(writer);

            for (Map.Entry<String, Gauge> entry : registry.getGauges(filter).entrySet()) {
                LabelParsedMetric lpm = parseMetricNameForLabels(entry.getKey());
                String sanitizedName = DropwizardMetricsExporter.sanitizeMetricName(lpm.getStrippedName());

                if (filtered.isEmpty() || filtered.contains(sanitizedName)) {
                    exporter.writeGauge(lpm.getStrippedName(), entry.getValue(), lpm.getLabels());
                }
            }
            for (Map.Entry<String, Counter> entry : registry.getCounters(filter).entrySet()) {
                LabelParsedMetric lpm = parseMetricNameForLabels(entry.getKey());
                String sanitizedName = DropwizardMetricsExporter.sanitizeMetricName(lpm.getStrippedName());

                if (filtered.isEmpty() || filtered.contains(sanitizedName)) {
                    exporter.writeCounter(entry.getKey(), entry.getValue(), lpm.getLabels());
                }
            }
            for (Map.Entry<String, Histogram> entry : registry.getHistograms(filter).entrySet()) {
                LabelParsedMetric lpm = parseMetricNameForLabels(entry.getKey());
                String sanitizedName = DropwizardMetricsExporter.sanitizeMetricName(lpm.getStrippedName());

                if (filtered.isEmpty() || filtered.contains(sanitizedName)) {
                    exporter.writeHistogram(entry.getKey(), entry.getValue(), lpm.getLabels());
                }
            }
            for (Map.Entry<String, Meter> entry : registry.getMeters(filter).entrySet()) {
                LabelParsedMetric lpm = parseMetricNameForLabels(entry.getKey());
                String sanitizedName = DropwizardMetricsExporter.sanitizeMetricName(lpm.getStrippedName());

                if (filtered.isEmpty() || filtered.contains(sanitizedName)) {
                    exporter.writeMeter(entry.getKey(), entry.getValue(), lpm.getLabels());
                }
            }
            for (Map.Entry<String, Timer> entry : registry.getTimers(filter).entrySet()) {
                LabelParsedMetric lpm = parseMetricNameForLabels(entry.getKey());
                String sanitizedName = DropwizardMetricsExporter.sanitizeMetricName(lpm.getStrippedName());

                if (filtered.isEmpty() || filtered.contains(sanitizedName)) {
                    exporter.writeTimer(entry.getKey(), entry.getValue(), lpm.getLabels());
                }
            }

            writer.flush();
        } catch (RuntimeException ex) {
            LOG.error("Unhandled exception", ex);
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        } finally {
            writer.close();
        }
    }
    private LabelParsedMetric parseMetricNameForLabels(String metricName) {
        StringBuffer sBuff  = new StringBuffer();
        Map<String, String> metricLabels = new HashMap<>();

        // Extract labels by pattern, and replace with empty string
        Matcher m = LABEL_PATTERN.matcher(metricName);

        while (m.find()) {
            String label = m.group(1).trim();

            Matcher kvm = KV_PAIR_PATTERN.matcher(label);

            if (kvm.matches()) {
                String key = kvm.group(1);
                String value = kvm.group(2);
                LOG.debug("Detected metric label: [%s, %s]", key, value);

                metricLabels.put(key, value);

            } else {
                LOG.warn("Metric Label does not match expected pattern. [%s]", KV_PAIR_PATTERN.pattern());
            }

            m.appendReplacement(sBuff, "");
        }

        m.appendTail(sBuff);

        // collapse all repeating "." characters to a single
        String result = MULTIDOT_PATTERN.matcher(sBuff.toString()).replaceAll(".");

        int start = 0;
        int end = result.length();

        // finally strip off leading or following "." chars
        if (result.startsWith(".")) {
            start = 1;
        }

        if (result.endsWith(".")) {
            end -= 1;
        }

        result = result.substring(start, end);
        LOG.debug("Final metric after label extraction [%s]", result);

        return new LabelParsedMetric(metricLabels, result);
    }

    private Set<String> parse(HttpServletRequest req) {
        String[] includedParam = req.getParameterValues("name[]");
        return includedParam == null ? Collections.<String>emptySet() : new HashSet<String>(Arrays.asList(includedParam));
    }

    private class LabelParsedMetric {
        private final Map<String, String> labels;
        private final String strippedName;

        public LabelParsedMetric(Map<String, String> labels, String strippedName) {

            this.labels = labels;
            this.strippedName = strippedName;
        }

        public String getStrippedName() {
            return strippedName;
        }

        public Map<String, String> getLabels() {
            return labels;
        }
    }

    public static void main(String[] args) throws Exception {
        String test = "aa.{organisation:org-1}.{bundleName:b-1}.bb.{bundleVersion:v-1}";
        StringBuffer sBuff  = new StringBuffer();

        Matcher m = LABEL_PATTERN.matcher(test);

        while (m.find()) {
            System.out.println(m.group(1));
            m.appendReplacement(sBuff, "");
        }

        m.appendTail(sBuff);

        String result = MULTIDOT_PATTERN.matcher(sBuff.toString()).replaceAll(".");

        int start = 0;
        int end = result.length();

        if (result.startsWith(".")) {
            start = 1;
        }

        if (result.endsWith(".")) {
            end -= 1;
        }

        System.out.println(result.substring(start, end));
    }
}

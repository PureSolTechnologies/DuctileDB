package com.puresoltechnologies.ductiledb.commons;

import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.JvmAttributeGaugeSet;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;

/**
 * This is the central class for distributing the metrics registry to the whole
 * application.
 * 
 * @author Rick-Rainer Ludwig
 */
public class Metrics {

    private static MetricRegistry registry;

    public static void initialize(MetricRegistry registry) {
	if (Metrics.registry != null) {
	    throw new IllegalStateException("Metrics were already initialized.");
	}
	Metrics.registry = registry;
	registry.register("memory_usage", new MemoryUsageGaugeSet());
	registry.register("garbage_collector", new GarbageCollectorMetricSet());
	registry.register("jvm_attributes", new JvmAttributeGaugeSet());

	final JmxReporter jmxReporter = JmxReporter.forRegistry(registry).build();
	jmxReporter.start();

	final Slf4jReporter slf4jReporter = Slf4jReporter.forRegistry(registry)
		.outputTo(LoggerFactory.getLogger("com.puresoltechnologies.famility.server.metrics"))
		.convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build();
	slf4jReporter.start(1, TimeUnit.MINUTES);
    }

    public static MetricRegistry getMetrics() {
	if (Metrics.registry == null) {
	    throw new IllegalStateException("Metrics were not initialized, yet.");
	}
	return registry;
    }

    /**
     * Private constructor to avoid instantiation.
     */
    private Metrics() {
    }
}

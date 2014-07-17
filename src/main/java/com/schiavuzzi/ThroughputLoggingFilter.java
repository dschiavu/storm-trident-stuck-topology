package com.schiavuzzi;

import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.IMetricsContext;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * A filter which counts number of tuples passing every configurable number of seconds.
 * Metrics are output to the logger (under the DEBUG log level),
 * as well to the Storm Metrics stream (only the windowed throughput).
 * Totals are also counted.
 * 
 * @author dschiavuzzi
 */
public class ThroughputLoggingFilter extends BaseFilter {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(ThroughputLoggingFilter.class);
	
	private static final int DEFAULT_RESOLUTION_SECONDS = 5;
	private static final int DEFAULT_BUCKET_SIZE_SECONDS = 10;
	
	private long resolutionSeconds; // Throughput resolution (calculation time window)
	private String prefix;		 // Metric name prefix

	private long totalCount;
	private long windowedCount;
	private long start;
	private long last;

	private transient ReducedMetric _throughputMetric;
	
	public ThroughputLoggingFilter() {
		this(DEFAULT_RESOLUTION_SECONDS);
	}
	
	public ThroughputLoggingFilter(String prefix) {
		this(DEFAULT_RESOLUTION_SECONDS, prefix);
	}
	
	public ThroughputLoggingFilter(int resolutionSeconds) {
		super();
		this.resolutionSeconds = resolutionSeconds;
	}
	
	public ThroughputLoggingFilter(int seconds, String prefix) {
		this(seconds);
		this.prefix = prefix;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
		this.start = System.nanoTime();
		this.last = System.nanoTime();
		this.totalCount = 0;
		this.windowedCount = 0;
		
		registerMetrics(context);
	}
	
	private void registerMetrics(IMetricsContext context) {
		_throughputMetric = new ReducedMetric(new MeanReducer());
        // TODO make the metrics time bucket size configurable
		context.registerMetric(
                metricPrefix() + "throughputTuplesPerSec",
                _throughputMetric,
                DEFAULT_BUCKET_SIZE_SECONDS
        );
	}

	private String metricPrefix() {
		return StringUtils.defaultString(prefix);
	}	
	
	@Override
	public boolean isKeep(final TridentTuple tuple) {
		totalCount += 1;
		windowedCount += 1;
		final long now = System.nanoTime();
		if (now - last > (resolutionSeconds * 1000000000L)) {
			long tuplesPerSec = (totalCount * 1000000000L) / (now - start);
			long partialTuplesPerSec = (windowedCount / resolutionSeconds);
			
			_throughputMetric.update(partialTuplesPerSec);

			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format(
                        "%s WINDOW (%d sec): %d tuples/s (%d total), TOTAL: %d tuples/s (%d total)",
                        metricPrefix(),
                        resolutionSeconds,
                        partialTuplesPerSec,
                        windowedCount,
                        tuplesPerSec,
                        totalCount
                ));
			}

			last = now;
			windowedCount = 0;
		}
		
		return true;
	}
}

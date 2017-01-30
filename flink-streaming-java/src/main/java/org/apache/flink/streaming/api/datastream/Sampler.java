package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.functions.sink.CollectSinkFunction;
import org.apache.flink.util.Preconditions;

/**
 * Created by Evgeny_Kincharov on 1/16/2017.
 */
public class Sampler<T> {

	private CollectSinkFunction<T> collector;

	public Iterable<T> getData(Time interval) throws InterruptedException {
		Preconditions.checkNotNull(collector, "Collector is not defined");
		Preconditions.checkNotNull(interval, "Interval shouldn't be null");
		return collector.getCollectedData(interval);
	}

	public void setCollector(CollectSinkFunction<T> collector) {
		this.collector = collector;
	}
}

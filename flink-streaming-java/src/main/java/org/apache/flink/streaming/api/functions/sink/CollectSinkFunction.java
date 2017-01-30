package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.Sampler;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

/**
 * Created by Evgeny_Kincharov on 1/10/2017.
 */
public class CollectSinkFunction<IN> extends RichSinkFunction<IN> implements SinkFunction<IN>{
	private static final long serialVersionUID = 1L;

	private Logger LOG = LoggerFactory.getLogger(this.getClass());

	private List<IN> data = new LinkedList<>();
	private long lastTimestamp = -1L;
	private Time timeToCollect;

	/**
	 * Instantiates a collect sink function that collect messages that are going trough.
	 */
	public CollectSinkFunction(Sampler<IN> sampler) {
		sampler.setCollector(this);
	}

	/*@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		//StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
	}*/

	@Override
	public void invoke(IN record) {
		LOG.info("invoke (before add): now: {}, last: {}, object: {}", System.currentTimeMillis(), lastTimestamp, Integer.toHexString(System.identityHashCode(this)));
		if(System.currentTimeMillis() <= lastTimestamp){
			LOG.info("invoke (ADD)");
			data.add(record);
		}
		LOG.info("invoke (after add): now: {}, last: {}, object {}", System.currentTimeMillis(), lastTimestamp, Integer.toHexString(System.identityHashCode(this)));
	}

	@Override
	public void close() {
		this.data = null;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder("Collect");
		if(lastTimestamp == -1L){
			result.append(" (disabled)");
		} else {
			Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
			calendar.setTimeInMillis(lastTimestamp - timeToCollect.toMilliseconds());
			String fromDate = String.format("%tF %<tT", calendar);
			calendar.setTimeInMillis(lastTimestamp);
			String toDate = String.format("%tF %<tT", calendar);
			result.append(" from ").
				append(fromDate).
				append(" to ").
				append(toDate);
		}
		return result.toString();
	}

	private void setTime(Time time){
		if (time == null){
			lastTimestamp = -1L;
			timeToCollect = null;
		} else {
			lastTimestamp = -1L;
			timeToCollect = time;
			lastTimestamp = System.currentTimeMillis() + time.toMilliseconds();
			LOG.info("Set logger to {}, object {}", lastTimestamp, Integer.toHexString(System.identityHashCode(this)));
		}
	}

	public List<IN> getCollectedData(Time interval) throws InterruptedException {
		setTime(null);
		data.clear();
		setTime(interval);
		LOG.info("Before wait, object {}", Integer.toHexString(System.identityHashCode(this)));
		Thread.sleep(interval.toMilliseconds());
		//this.wait(interval.toMilliseconds());
		LOG.info("After wait, object {}", Integer.toHexString(System.identityHashCode(this)));
		//sleep(Math.max(lastTimestamp - System.currentTimeMillis(), 0L));
		return data;
	}

	private void writeObject(ObjectOutputStream stream) throws IOException {
		LOG.info("Before write {}", Integer.toHexString(System.identityHashCode(this)));
		stream.defaultWriteObject();
		LOG.info("After write {}", Integer.toHexString(System.identityHashCode(this)));
	}

	private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
		stream.defaultReadObject();
		LOG.info("After read {}", Integer.toHexString(System.identityHashCode(this)));
	}
}

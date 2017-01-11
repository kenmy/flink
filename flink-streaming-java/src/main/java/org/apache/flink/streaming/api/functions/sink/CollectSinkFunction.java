package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.api.common.time.Time;

import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

/**
 * Created by Evgeny_Kincharov on 1/10/2017.
 */
public class CollectSinkFunction<IN> extends RichSinkFunction<IN> implements SinkFunction<IN>{
	private static final long serialVersionUID = 1L;

	private List<IN> data = new LinkedList<>();
	private long lastTimestamp = -1L;
	private Time timeToCollect;

	/**
	 * Instantiates a collect sink function that collect messages that are going trough.
	 */
	public CollectSinkFunction() {

	}

	/*@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		//StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
	}*/

	@Override
	public void invoke(IN record) {
		if(System.currentTimeMillis() > lastTimestamp){
			data.add(record);
		}
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
		}
	}

	public List<IN> getCollectedData(Time interval) throws InterruptedException {
		setTime(null);
		data.clear();
		setTime(interval);
		Thread.sleep(Math.max(lastTimestamp - System.currentTimeMillis(), 0L));
		return data;
	}
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.sink.CollectSinkFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

@Internal
public class CollectFilter<IN> extends AbstractUdfStreamOperator<IN, CollectSinkFunction> implements OneInputStreamOperator<IN, IN> {

	private static final long serialVersionUID = 1L;

	private Logger LOG = LoggerFactory.getLogger(this.getClass());

	public CollectFilter(CollectSinkFunction<IN> collector) {
		super(collector);
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		LOG.info("processElement (before invoke)");
		userFunction.invoke(element.getValue());
		LOG.info("processElement (after invoke)");
		output.collect(element);
	}
}

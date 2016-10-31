/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.hadoopcompatibility;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * The HadoopInputs is the utility class for create {@link HadoopInputFormat}.
 *
 * Methods:
 * createHadoopInput - create {@link org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat} or {@link org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat}
 * by {@link org.apache.hadoop.mapred.FileInputFormat} or {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat}, key class, value class.
 *
 * readHadoopFile - run createHadoopInput and add input path to {@link org.apache.hadoop.mapred.JobConf} or
 * {@link org.apache.hadoop.mapreduce.Job}
 *
 */
public final class HadoopInputs {
	// ----------------------------------- Hadoop Input Format ---------------------------------------

	/**
	 * Creates a {@link HadoopInputFormat} from the given {@link org.apache.hadoop.mapred.FileInputFormat}. The
	 * given inputName is set on the given job.
	 */
	@PublicEvolving
	public static <K,V> HadoopInputFormat<K, V> readHadoopFile(org.apache.hadoop.mapred.FileInputFormat<K,V> mapredInputFormat, Class<K> key, Class<V> value, String inputPath, JobConf job) {
		HadoopInputFormat<K, V> result = createHadoopInput(mapredInputFormat, key, value, job);

		org.apache.hadoop.mapred.FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(inputPath));

		return result;
	}

	/**
	 * Creates a {@link HadoopInputFormat} from {@link org.apache.hadoop.mapred.SequenceFileInputFormat}
	 * A {@link org.apache.hadoop.mapred.JobConf} with the given inputPath is created.
	 */
	@PublicEvolving
	public static <K,V> HadoopInputFormat<K, V> readSequenceFile(Class<K> key, Class<V> value, String inputPath) throws IOException {
		return readHadoopFile(new org.apache.hadoop.mapred.SequenceFileInputFormat<K, V>(), key, value, inputPath);
	}

	/**
	 * Creates a {@link HadoopInputFormat} from the given {@link org.apache.hadoop.mapred.FileInputFormat}. A
	 * {@link org.apache.hadoop.mapred.JobConf} with the given inputPath is created.
	 */
	@PublicEvolving
	public static <K,V> HadoopInputFormat<K, V> readHadoopFile(org.apache.hadoop.mapred.FileInputFormat<K,V> mapredInputFormat, Class<K> key, Class<V> value, String inputPath) {
		return readHadoopFile(mapredInputFormat, key, value, inputPath, new JobConf());
	}

	/**
	 * Creates a {@link HadoopInputFormat} from the given {@link org.apache.hadoop.mapred.InputFormat}.
	 */
	@PublicEvolving
	public static <K,V> HadoopInputFormat<K, V> createHadoopInput(org.apache.hadoop.mapred.InputFormat<K,V> mapredInputFormat, Class<K> key, Class<V> value, JobConf job) {
		return new HadoopInputFormat<>(mapredInputFormat, key, value, job);
	}

	/**
	 * Creates a {@link org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat} from the given {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat}. The
	 * given inputName is set on the given job.
	 */
	@PublicEvolving
	public static <K,V> org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat<K, V> readHadoopFile(org.apache.hadoop.mapreduce.lib.input.FileInputFormat<K,V> mapreduceInputFormat, Class<K> key, Class<V> value, String inputPath, Job job) throws IOException {
		org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat<K, V> result = createHadoopInput(mapreduceInputFormat, key, value, job);

		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new org.apache
			.hadoop.fs.Path(inputPath));

		return result;
	}

	/**
	 * Creates a {@link org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat} from the given {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat}. A
	 * {@link org.apache.hadoop.mapreduce.Job} with the given inputPath is created.
	 */
	@PublicEvolving
	public static <K,V> org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat<K, V> readHadoopFile(org.apache.hadoop.mapreduce.lib.input.FileInputFormat<K,V> mapreduceInputFormat, Class<K> key, Class<V> value, String inputPath) throws IOException {
		return readHadoopFile(mapreduceInputFormat, key, value, inputPath, Job.getInstance());
	}

	/**
	 * Creates a {@link org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat} from the given {@link org.apache.hadoop.mapreduce.InputFormat}.
	 */
	@PublicEvolving
	public static <K,V> org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat<K, V> createHadoopInput(org.apache.hadoop.mapreduce.InputFormat<K,V> mapreduceInputFormat, Class<K> key, Class<V> value, Job job) {
		return new org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat<>(mapreduceInputFormat, key, value, job);
	}
}

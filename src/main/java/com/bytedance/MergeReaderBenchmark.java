/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bytedance;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.mergetree.compact.ReducerMergeFunctionWrapper;
import org.apache.paimon.mergetree.compact.SortMergeReader;
import org.apache.paimon.mergetree.compact.SortMergeReaderV2;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.Pair;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

/**
 * @created: 2023/3/31
 */
@State(Scope.Group)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 1, jvmArgsAppend = {
		"-Djava.rmi.server.hostname=127.0.0.1",
		"-Dcom.sun.management.jmxremote.authenticate=false",
		"-Dcom.sun.management.jmxremote.ssl=false",
		"-Dcom.sun.management.jmxremote.ssl"})
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 10, time = 10)
public class MergeReaderBenchmark {
	private static final RecordComparator KEY_COMPARATOR = (a, b) -> a.getString(0).compareTo(b.getString(0));

	@Param({"2", "5", "10", "20", "50"})
	private int readersNum;

	@Param({"1000", "10000", "100000"})
	private int recordNum;

	List<ReusingRecordReader<BinaryString, Long>> readersV1;
	List<ReusingRecordReader<BinaryString, Long>> readersV2;

	private List<List<ReusingTestData<BinaryString, Long>>> testData;

	private List<ReusingTestData<BinaryString, Long>> expectedData;

	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.shouldDoGC(true)
				.include(".*" + MergeReaderBenchmark.class.getCanonicalName() + ".*")
				.build();
		new Runner(options).run();
	}

	@Setup(Level.Iteration)
	public void beforeIteration() {
		testData = generateRandomData();
		readersV1 = new ArrayList<>(readersNum);
		readersV2 = new ArrayList<>(readersNum);
		for (List<ReusingTestData<BinaryString, Long>> readerData : testData) {
			List<Pair<Integer, Integer>> sequence = ReusingRecordReader.generateSequence(readerData.size());
			readersV1.add(new ReusingRecordReader<>(
					readerData,
					sequence,
					ReusingKeyValue::new,
					(keyWriter, key) -> {
						keyWriter.writeString(0, key);
						keyWriter.complete();
						},
					(valueWriter, value) -> {
						valueWriter.writeLong(0, value);
						valueWriter.complete();
					}));
			readersV2.add(new ReusingRecordReader<>(
					readerData,
					sequence,
					ReusingKeyValue::new,
					(keyWriter, key) -> {
						keyWriter.writeString(0, key);
						keyWriter.complete();
					},
					(valueWriter, value) -> {
						valueWriter.writeLong(0, value);
						valueWriter.complete();
					}));
		}
		expectedData = getExpectedForDeduplicate(
				testData.stream()
						.flatMap(Collection::stream)
						.collect(Collectors.toList()));
	}

	@Benchmark
	@Group
	public void minHeap(Blackhole blackhole) throws IOException {
		readersV1.forEach(ReusingRecordReader::reset);
		try(RecordReader<KeyValue> recordReader = new SortMergeReader<>(
				new ArrayList<>(readersV1),
				KEY_COMPARATOR,
				new ReducerMergeFunctionWrapper(DeduplicateMergeFunction.factory().create()))) {
			readBatch(recordReader, blackhole);
		}
	}

	@Benchmark
	@Group
	public void loserTree(Blackhole blackhole) throws IOException {
		readersV2.forEach(ReusingRecordReader::reset);
		try(RecordReader<KeyValue> recordReader = new SortMergeReaderV2<>(
				new ArrayList<>(readersV2),
				KEY_COMPARATOR,
				new ReducerMergeFunctionWrapper(DeduplicateMergeFunction.factory().create()))) {
			readBatch(recordReader, blackhole);
		}
	}

	private void readBatch(RecordReader<KeyValue> recordReader, Blackhole blackhole) throws IOException {
		RecordReader.RecordIterator<KeyValue> batch;
		Iterator<ReusingTestData<BinaryString, Long>> expectedIterator = expectedData.iterator();
		while ((batch = recordReader.readBatch()) != null) {
			KeyValue kv;
			while ((kv = batch.next()) != null) {
//				Assertions.assertThat(expectedIterator.hasNext()).isTrue();
//				ReusingTestData<BinaryString, Long> expected = expectedIterator.next();
//				expected.assertEquals(kv, keyValue -> keyValue.key().getString(0), keyValue -> keyValue.value().getLong(0));
				blackhole.consume(kv);
			}
			batch.releaseBatch();
		}
	}

	private List<List<ReusingTestData<BinaryString, Long>>> generateRandomData() {
		LinkedHashSet<BinaryString> predefinedKeys = new LinkedHashSet<>(recordNum * 3);
		while (predefinedKeys.size() < recordNum * 3) {
			predefinedKeys.add(BinaryString.fromString(RandomStringUtils.randomAlphabetic(128)));
		}

		List<List<ReusingTestData<BinaryString, Long>>> readersData = new ArrayList<>();
		for (int i = 0; i < readersNum; i++) {
			readersData.add(
					ReusingTestData.generateOrderedNoDuplicatedKeys(
							recordNum,
							false,
							new ArrayList<>(predefinedKeys),
							random -> {
								int value = random.nextInt(10) - 5;
								while (value == 0) {
									value = random.nextInt(10) - 5;
								}
								return (long) value;
							}));
		}
		return readersData;
	}

	@TearDown(Level.Iteration)
	public void afterIteration() throws InterruptedException, IOException {
		System.gc();
		Thread.sleep(1000L);
	}

	public static <K extends Comparable<K>, V> List<ReusingTestData<K, V>> getExpectedForDeduplicate(
			List<ReusingTestData<K, V>> input) {
		input = new ArrayList<>(input);
		input.sort(null);

		List<ReusingTestData<K, V>> expected = new ArrayList<>();
		for (int i = 0; i < input.size(); i++) {
			ReusingTestData<K, V> data = input.get(i);
			if (i + 1 >= input.size() || data.key != input.get(i + 1).key) {
				expected.add(data);
			}
		}
		return expected;
	}
}

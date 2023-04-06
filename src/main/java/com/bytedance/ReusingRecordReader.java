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

package com.bytedance;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A testing {@link RecordReader} using {@link ReusingTestData} which produces batches of random
 * sizes (possibly empty). {@link KeyValue}s produced by the same reader is reused to test that
 * other components correctly handles the reusing.
 */
public class ReusingRecordReader<K extends Comparable<K>, V> implements RecordReader<KeyValue> {

    private final List<ReusingTestData<K, V>> testData;
    private final List<Pair<Integer, Integer>> sequence;

    private final List<TestRecordIterator> producedBatches;

    private final Supplier<ReusingKeyValue> reuseFactory;
    private final BiConsumer<BinaryRowWriter, K> keyUpdater;
    private final BiConsumer<BinaryRowWriter, V> valueUpdater;

    private ReusingKeyValue reuse;
    private int nextSequence;
    private boolean closed;

    public ReusingRecordReader(
            List<ReusingTestData<K, V>> testData,
            List<Pair<Integer, Integer>> sequence,
            Supplier<ReusingKeyValue> reuseFactory,
            BiConsumer<BinaryRowWriter, K> keyUpdater,
            BiConsumer<BinaryRowWriter, V> valueUpdater) {
        this.testData = testData;
        this.sequence = sequence;
        this.producedBatches = new ArrayList<>(sequence.size());
        this.reuseFactory = reuseFactory;
        this.keyUpdater = keyUpdater;
        this.valueUpdater = valueUpdater;
        this.reuse = reuseFactory.get();
        this.nextSequence = 0;
        this.closed = false;
    }

    public static List<Pair<Integer, Integer>> generateSequence(int maxSize) {
        List<Pair<Integer, Integer>> sequence = new ArrayList<>();
        int nextLowerBound = 0;
        Random random = new Random();
        while (nextLowerBound < maxSize || !random.nextBoolean()) {
            int upperBound = random.nextInt(maxSize - nextLowerBound + 1) + nextLowerBound;
            sequence.add(Pair.of(nextLowerBound, upperBound));
            nextLowerBound = upperBound;
        }
        return sequence;
    }

    @Nullable
    @Override
    public RecordIterator<KeyValue> readBatch() {
        assertThat(nextSequence <= sequence.size());
        TestRecordIterator iterator = null;
        if (nextSequence < sequence.size()) {
            Pair<Integer, Integer> offsetPair = sequence.get(nextSequence);
            iterator = new TestRecordIterator(offsetPair.getLeft(), offsetPair.getRight());
            producedBatches.add(iterator);
        }
        nextSequence++;
        return iterator;
    }

    public void reset() {
        closed = false;
        producedBatches.clear();
        nextSequence = 0;
        reuse = reuseFactory.get();
    }

    @Override
    public void close() throws IOException {
        closed = true;
        assertCleanUp();
    }

    public void assertCleanUp() {
        assertThat(closed).isTrue();
        for (TestRecordIterator iterator : producedBatches) {
            assertThat(iterator.released).isTrue();
        }
    }

    private class TestRecordIterator implements RecordIterator<KeyValue> {

        private final int upperBound;

        private int next;
        private boolean released;

        private TestRecordIterator(
                int lowerBound,
                int upperBound) {
            this.upperBound = upperBound;;

            this.next = lowerBound;
            this.released = false;
        }

        @Override
        public KeyValue next() throws IOException {
            assertThat(next != -1).isTrue();
            if (next == upperBound) {
                next = -1;
                return null;
            }
            KeyValue result = reuse.update(testData.get(next), keyUpdater, valueUpdater);
            next++;
            return result;
        }

        @Override
        public void releaseBatch() {
            this.released = true;
        }
    }
}

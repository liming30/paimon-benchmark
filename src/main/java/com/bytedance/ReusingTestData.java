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
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.ReusingKeyValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A simple test data structure which is mainly used for testing if other components handle reuse of
 * {@link KeyValue} correctly. Use along with {@link ReusingKeyValue}.
 */
public class ReusingTestData<K extends Comparable<K>, V> implements Comparable<ReusingTestData<K, V>> {

    public final K key;
    public final long sequenceNumber;
    public final RowKind valueKind;
    public final V value;

    public ReusingTestData(K key, long sequenceNumber, RowKind valueKind, V value) {
        this.key = key;
        this.sequenceNumber = sequenceNumber;
        this.valueKind = valueKind;
        this.value = value;
    }

    @Override
    public int compareTo(ReusingTestData<K, V> that) {
        if (key != that.key) {
            return key.compareTo(that.key);
        }
        int result = Long.compare(sequenceNumber, that.sequenceNumber);
        Preconditions.checkArgument(
                result != 0 || this == that,
                "Found two CompactTestData with the same sequenceNumber. This is invalid.");
        return result;
    }

    public void assertEquals(KeyValue kv, Function<KeyValue, K> keyFunction, Function<KeyValue, V> valueFunction) {
        assertThat(keyFunction.apply(kv)).isEqualTo(key);
        assertThat(kv.sequenceNumber()).isEqualTo(sequenceNumber);
        assertThat(kv.valueKind()).isEqualTo(valueKind);
        assertThat(valueFunction.apply(kv)).isEqualTo(value);
    }

    /**
     * String format:
     *
     * <ul>
     *   <li>Use | to split different {@link KeyValue}s.
     *   <li>For a certain key value, the format is <code>
     *       key, sequenceNumber, valueKind (+ or -), value</code>.
     * </ul>
     */
    public static <K extends Comparable<K>, V> List<ReusingTestData<K, V>> parse(
            String s,
            Function<String, K> keyParser,
            Function<String, V> valueParser) {
        List<ReusingTestData<K, V>> result = new ArrayList<>();
        for (String kv : s.split("\\|")) {
            if (kv.trim().isEmpty()) {
                continue;
            }
            String[] split = kv.split(",");
            Preconditions.checkArgument(split.length == 4, "Found invalid data string " + kv);
            result.add(
                    new ReusingTestData<>(
                            keyParser.apply(split[0].trim()),
                            Long.parseLong(split[1].trim()),
                            split[2].trim().equals("+") ? RowKind.INSERT : RowKind.DELETE,
                            valueParser.apply(split[3].trim())));
        }
        return result;
    }

    public static <K extends Comparable<K>, V> List<ReusingTestData<K, V>> generateData(
            int numRecords,
            boolean onlyAdd,
            Function<Random, K> randomKeyGenerator,
            Function<Random, V> randomValueGenerator) {
        List<ReusingTestData<K, V>> result = new ArrayList<>();
        Map<Integer, K> randomKeys = new HashMap<>();
        SequenceNumberGenerator generator = new SequenceNumberGenerator();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < numRecords; i++) {
            int index = random.nextInt(numRecords);
            K key = randomKeys.computeIfAbsent(index, k -> randomKeyGenerator.apply(random));
            V value = randomValueGenerator.apply(random);
            result.add(
                    new ReusingTestData<>(
                            key,
                            generator.next(),
                            random.nextBoolean() || onlyAdd ? RowKind.INSERT : RowKind.DELETE,
                            value));
        }
        return result;
    }

    public static <K extends Comparable<K>, V> List<ReusingTestData<K, V>> generateOrderedNoDuplicatedKeys(
            int numRecords,
            boolean onlyAdd,
            List<K> predefinedKeys,
            Function<Random, V> randomValueGenerator) {
        TreeMap<K, ReusingTestData<K, V>> result = new TreeMap<>();
        SequenceNumberGenerator generator = new SequenceNumberGenerator();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        while (result.size() < numRecords) {
            int index = random.nextInt(predefinedKeys.size());
            K key = predefinedKeys.get(index);
            V value = randomValueGenerator.apply(random);
            result.put(
                    key,
                    new ReusingTestData<>(
                            key,
                            generator.next(),
                            random.nextBoolean() || onlyAdd ? RowKind.INSERT : RowKind.DELETE,
                            value));
        }
        return new ArrayList<>(result.values());
    }

    private static class SequenceNumberGenerator {
        private final Set<Long> usedSequenceNumber;

        private SequenceNumberGenerator() {
            this.usedSequenceNumber = new HashSet<>();
        }

        public long next() {
            long result;
            do {
                result = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
            } while (usedSequenceNumber.contains(result));
            usedSequenceNumber.add(result);
            return result;
        }
    }

    @Override
    public String toString() {
        return "ReusingTestData{" +
                "key=" + key +
                ", sequenceNumber=" + sequenceNumber +
                ", valueKind=" + valueKind +
                ", value=" + value +
                '}';
    }
}

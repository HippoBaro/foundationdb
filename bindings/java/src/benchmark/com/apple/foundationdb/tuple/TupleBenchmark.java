/*
 * TupleBenchmark.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.tuple;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmarks for the Tuple layer.
 *
 * <p>These benchmarks measure both throughput and allocation characteristics
 * of Tuple operations. The allocation-related metrics are the primary concern:
 * the packed-bytes representation avoids boxing and intermediate object allocation,
 * which reduces GC pressure. Run with {@code -prof gc} to see per-operation
 * allocation rates ({@code gc.alloc.rate.norm}).
 *
 * <p>Data generation strategies are adapted from {@code TuplePerformanceTest}
 * to preserve coverage of the same element type distributions.
 *
 * <h3>Running</h3>
 * <pre>
 *   # Quick throughput run
 *   java -cp &lt;classpath&gt; org.openjdk.jmh.Main TupleBenchmark -f 1 -wi 3 -i 5
 *
 *   # Allocation-focused run (recommended)
 *   java -cp &lt;classpath&gt; org.openjdk.jmh.Main TupleBenchmark -f 2 -wi 5 -i 10 -prof gc
 *
 *   # Scan-loop benchmarks only, with GC profiling
 *   java -cp &lt;classpath&gt; org.openjdk.jmh.Main "TupleBenchmark.scanLoop*" -prof gc
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(2)
public class TupleBenchmark {

	// -----------------------------------------------------------------------
	// Data generation (adapted from TuplePerformanceTest)
	// -----------------------------------------------------------------------

	private static List<Object> generateElements(Random r, int count, String type) {
		switch (type) {
			case "LONGS":            return generateLongs(r, count);
			case "FLOATING_POINT":   return generateFloatingPoint(r, count);
			case "STRING_LIKE":      return generateStringLike(r, count);
			case "MIXED":            return generateMixed(r, count);
			default:
				throw new IllegalArgumentException("Unknown element type: " + type);
		}
	}

	private static List<Object> generateLongs(Random r, int count) {
		List<Object> items = new ArrayList<Object>(count);
		for (int i = 0; i < count; i++) {
			int byteLength = r.nextInt(Long.BYTES + 1);
			long val = 0L;
			for (int x = 0; x < byteLength; x++) {
				val = (val << 8) + r.nextInt(256);
			}
			items.add(val);
		}
		return items;
	}

	private static List<Object> generateFloatingPoint(Random r, int count) {
		List<Object> items = new ArrayList<Object>(count);
		for (int i = 0; i < count; i++) {
			double choice = r.nextDouble();
			if (choice < 0.40) {
				items.add(r.nextFloat());
			} else if (choice < 0.80) {
				items.add(r.nextDouble());
			} else if (choice < 0.90) {
				items.add(Float.intBitsToFloat(r.nextInt()));
			} else {
				items.add(Double.longBitsToDouble(r.nextLong()));
			}
		}
		return items;
	}

	private static List<Object> generateStringLike(Random r, int count) {
		List<Object> items = new ArrayList<Object>(count);
		for (int i = 0; i < count; i++) {
			double choice = r.nextDouble();
			if (choice < 0.4) {
				byte[] arr = new byte[r.nextInt(20)];
				r.nextBytes(arr);
				items.add(arr);
			} else if (choice < 0.8) {
				int[] codepoints = new int[r.nextInt(20)];
				for (int x = 0; x < codepoints.length; x++) {
					codepoints[x] = r.nextInt(0x7F);
				}
				items.add(new String(codepoints, 0, codepoints.length));
			} else if (choice < 0.9) {
				items.add(new byte[r.nextInt(20)]);
			} else {
				int[] codepoints = new int[r.nextInt(20)];
				for (int x = 0; x < codepoints.length; x++) {
					int cp = r.nextInt(0x10FFFF);
					while (Character.isSurrogate((char) cp)) {
						cp = r.nextInt(0x10FFFF);
					}
					codepoints[x] = cp;
				}
				items.add(new String(codepoints, 0, codepoints.length));
			}
		}
		return items;
	}

	private static List<Object> generateMixed(Random r, int count) {
		List<Object> items = new ArrayList<Object>(count);
		for (int i = 0; i < count; i++) {
			double choice = r.nextDouble();
			if (choice < 0.10) {
				items.add(null);
			} else if (choice < 0.20) {
				byte[] bytes = new byte[r.nextInt(20)];
				r.nextBytes(bytes);
				items.add(bytes);
			} else if (choice < 0.30) {
				char[] chars = new char[r.nextInt(20)];
				for (int j = 0; j < chars.length; j++) {
					chars[j] = (char) ('a' + r.nextInt(26));
				}
				items.add(new String(chars));
			} else if (choice < 0.45) {
				items.add(r.nextInt(10_000_000) * (r.nextBoolean() ? -1 : 1));
			} else if (choice < 0.55) {
				items.add(r.nextFloat());
			} else if (choice < 0.65) {
				items.add(r.nextDouble());
			} else if (choice < 0.75) {
				items.add(r.nextBoolean());
			} else if (choice < 0.85) {
				items.add(UUID.randomUUID());
			} else {
				// Nested tuple (small, to avoid deep recursion)
				List<Object> nested = new ArrayList<Object>(3);
				nested.add((long) r.nextInt(1000));
				char[] chars = new char[5];
				for (int j = 0; j < chars.length; j++) {
					chars[j] = (char) ('a' + r.nextInt(26));
				}
				nested.add(new String(chars));
				items.add(Tuple.fromList(nested));
			}
		}
		return items;
	}

	// -----------------------------------------------------------------------
	// State: parameterized tuple data
	// -----------------------------------------------------------------------

	@State(Scope.Thread)
	public static class TupleState {

		@Param({"1", "5", "10", "50", "100"})
		int tupleSize;

		@Param({"LONGS", "STRINGS", "MIXED", "FLOATING_POINT", "STRING_LIKE"})
		String elementType;

		/** Elements for construction benchmarks. */
		List<Object> elements;
		/** Pre-built tuple for access/comparison benchmarks. */
		Tuple preBuiltTuple;
		/** A distinct copy for equals/compareTo. */
		Tuple preBuiltTupleCopy;
		/** Packed bytes for fromBytes benchmarks. */
		byte[] packedBytes;
		/** Tuple built via fromList(getItems()) for equalsByValue. */
		Tuple valueBuiltTupleA;
		/** A distinct copy built via fromList(getItems()) for equalsByValue. */
		Tuple valueBuiltTupleB;

		@Setup(Level.Trial)
		public void setup() {
			Random r = new Random(42);
			if ("STRINGS".equals(elementType)) {
				// STRINGS: use ASCII strings (not in TuplePerformanceTest, but useful for benchmarking)
				elements = new ArrayList<Object>(tupleSize);
				for (int i = 0; i < tupleSize; i++) {
					char[] chars = new char[10 + r.nextInt(20)];
					for (int j = 0; j < chars.length; j++) {
						chars[j] = (char) ('a' + r.nextInt(26));
					}
					elements.add(new String(chars));
				}
			} else {
				elements = generateElements(r, tupleSize, elementType);
			}
			preBuiltTuple = Tuple.fromList(elements);
			packedBytes = preBuiltTuple.pack();
			preBuiltTupleCopy = Tuple.fromBytes(packedBytes);
			valueBuiltTupleA = Tuple.fromList(preBuiltTuple.getItems());
			valueBuiltTupleB = Tuple.fromList(preBuiltTupleCopy.getItems());
		}
	}

	// -----------------------------------------------------------------------
	// State: fresh tuple per invocation for first-time hashCode measurement
	// -----------------------------------------------------------------------

	@State(Scope.Thread)
	public static class HashCodeState {

		Tuple freshTuple;

		/** Trial setup: generate elements once. */
		private List<Object> elements;

		@Setup(Level.Trial)
		public void setupTrial(TupleState tupleState) {
			elements = tupleState.elements;
		}

		/** Invocation setup: build a fresh tuple so memoizedHash is 0. */
		@Setup(Level.Invocation)
		public void setupInvocation() {
			freshTuple = Tuple.fromList(elements);
		}
	}

	// -----------------------------------------------------------------------
	// State: large tuple for selective access benchmarks
	// -----------------------------------------------------------------------

	@State(Scope.Thread)
	public static class SelectiveAccessState {

		Tuple largeTuple;
		byte[] largePacked;

		@Setup(Level.Trial)
		public void setup() {
			Random r = new Random(42);
			List<Object> items = new ArrayList<Object>(100);
			for (int i = 0; i < 100; i++) {
				items.add((long) (i * 1000 + r.nextInt(1000)));
			}
			largeTuple = Tuple.fromList(items);
			largePacked = largeTuple.pack();
		}
	}

	// -----------------------------------------------------------------------
	// State: batch of packed tuples for scan-loop benchmarks
	// -----------------------------------------------------------------------

	@State(Scope.Thread)
	public static class BatchState {

		static final int BATCH_SIZE = 10_000;

		@Param({"10", "50"})
		int batchTupleSize;

		byte[][] packedBatch;

		@Setup(Level.Trial)
		public void setup() {
			Random r = new Random(42);
			packedBatch = new byte[BATCH_SIZE][];
			for (int i = 0; i < BATCH_SIZE; i++) {
				packedBatch[i] = Tuple.fromList(generateLongs(r, batchTupleSize)).pack();
			}
		}
	}

	// =======================================================================
	// Benchmarks ported from TuplePerformanceTest
	// =======================================================================

	/**
	 * Measures: pack() on a pre-built Tuple.
	 * (TuplePerformanceTest: packNanos)
	 *
	 * <p>New impl: near-free Arrays.copyOf of already-packed bytes.
	 */
	@Benchmark
	public byte[] pack(TupleState state) {
		return state.preBuiltTuple.pack();
	}

	/**
	 * Measures: Tuple.fromBytes() -- constructing a Tuple from packed bytes.
	 * (TuplePerformanceTest: unpackNanos)
	 *
	 * <p>New impl: structural scan only, no element decoding.
	 * Old impl: full decode of all elements into boxed objects.
	 * Run with {@code -prof gc} to see the allocation difference.
	 */
	@Benchmark
	public Tuple fromBytes(TupleState state) {
		return Tuple.fromBytes(state.packedBytes);
	}

	/**
	 * Measures: equals() on tuples rebuilt from getItems() (forces materialization).
	 * (TuplePerformanceTest: equalsNanos)
	 *
	 * <p>The tuples are constructed via fromList(getItems()) in setup, so this
	 * measures only the equals() call, not the materialization + repacking cost.
	 */
	@Benchmark
	public boolean equalsByValue(TupleState state) {
		return state.valueBuiltTupleA.equals(state.valueBuiltTupleB);
	}

	/**
	 * Measures: equals() on tuples that already have packed bytes.
	 * (TuplePerformanceTest: equalsArrayNanos)
	 *
	 * <p>New impl: unsigned byte array comparison (cache-friendly, no boxing).
	 */
	@Benchmark
	public boolean equalsByPacked(TupleState state) {
		return state.preBuiltTuple.equals(state.preBuiltTupleCopy);
	}

	/**
	 * Measures: getPackedSize().
	 * (TuplePerformanceTest: sizeNanos)
	 *
	 * <p>New impl: returns packed.length directly.
	 * Old impl: iterated over elements to sum sizes (first call).
	 */
	@Benchmark
	public int getPackedSize(TupleState state) {
		return state.preBuiltTuple.getPackedSize();
	}

	/**
	 * Measures: hashCode() on a freshly-constructed tuple (first computation).
	 * (TuplePerformanceTest: hashNanos)
	 *
	 * <p>Uses per-invocation state to ensure memoizedHash is always 0,
	 * isolating the hash computation from tuple construction cost.
	 */
	@Benchmark
	public int tupleHashCode(HashCodeState state) {
		return state.freshTuple.hashCode();
	}

	/**
	 * Measures: hashCode() memoized (second call on same object).
	 * (TuplePerformanceTest: secondHashNanos)
	 */
	@Benchmark
	public int tupleHashCodeMemoized(TupleState state) {
		return state.preBuiltTuple.hashCode();
	}

	// =======================================================================
	// Scan-loop benchmarks (batch operations where GC effects compound)
	// =======================================================================

	/**
	 * Simulates a realistic scan loop: decode packed bytes and access one typed field.
	 *
	 * <p>Over 10K iterations, the old impl would allocate ~10K ArrayLists + ~10K*N
	 * boxed objects, triggering GC. The new impl allocates only byte[] copies +
	 * offset arrays. With {@code -prof gc}, compare gc.count and gc.time.
	 */
	@Benchmark
	@BenchmarkMode(Mode.SingleShotTime)
	@OutputTimeUnit(TimeUnit.MILLISECONDS)
	@Warmup(iterations = 5, batchSize = 1)
	@Measurement(iterations = 20, batchSize = 1)
	public void scanLoopTypedAccess(BatchState state, Blackhole bh) {
		for (byte[] packed : state.packedBatch) {
			Tuple t = Tuple.fromBytes(packed);
			bh.consume(t.getLong(0));
		}
	}

	/**
	 * Simulates a scan loop with full materialization -- the worst case for the
	 * new implementation. Both old and new must allocate the full object graph here.
	 */
	@Benchmark
	@BenchmarkMode(Mode.SingleShotTime)
	@OutputTimeUnit(TimeUnit.MILLISECONDS)
	@Warmup(iterations = 5, batchSize = 1)
	@Measurement(iterations = 20, batchSize = 1)
	public void scanLoopFullMaterialization(BatchState state, Blackhole bh) {
		for (byte[] packed : state.packedBatch) {
			Tuple t = Tuple.fromBytes(packed);
			bh.consume(t.getItems());
		}
	}

	// =======================================================================
	// Throughput benchmarks for new implementation advantages
	// =======================================================================

	/**
	 * Measures: fromList() + pack() -- the common "construct and serialize" path.
	 *
	 * <p>New impl: packs in a single allocation (size pre-computed).
	 * Old impl: stores in ArrayList, then encodes lazily on pack().
	 */
	@Benchmark
	public byte[] fromListThenPack(TupleState state) {
		return Tuple.fromList(state.elements).pack();
	}

	/**
	 * Measures: building a tuple via add() chain, then packing.
	 *
	 * <p>Both impls are O(N^2) here (copying the backing store each time).
	 * New copies byte arrays; old copies ArrayLists.
	 */
	@Benchmark
	public byte[] addChainThenPack(TupleState state) {
		Tuple t = new Tuple();
		for (Object item : state.elements) {
			t = t.addObject(item);
		}
		return t.pack();
	}

	/**
	 * Measures: fromBytes() + access a single typed element.
	 *
	 * <p>New impl: structural scan in fromBytes(), then builds offset index +
	 * decodes only element 0. Old impl: decodes all N elements in fromBytes().
	 */
	@Benchmark
	public long fromBytesThenGetLong(TupleState state) {
		Tuple t = Tuple.fromBytes(state.packedBytes);
		return t.getLong(0);
	}

	/**
	 * Measures: fromBytes() on a 100-element tuple, then access element 50.
	 *
	 * <p>This is the key selective-access advantage: the new impl decodes only
	 * 1 of 100 elements. The old impl decoded all 100 in fromBytes().
	 */
	@Benchmark
	public long fromBytesThenGetMiddle(SelectiveAccessState state) {
		Tuple t = Tuple.fromBytes(state.largePacked);
		return t.getLong(50);
	}

	/**
	 * Measures: typed access to the first element of a pre-built 100-element tuple.
	 */
	@Benchmark
	public long getFirstElement(SelectiveAccessState state) {
		return state.largeTuple.getLong(0);
	}

	/**
	 * Measures: typed access to element 50 of a pre-built 100-element tuple.
	 */
	@Benchmark
	public long getMiddleElement(SelectiveAccessState state) {
		return state.largeTuple.getLong(50);
	}

	/**
	 * Measures: typed access to the last element of a pre-built 100-element tuple.
	 */
	@Benchmark
	public long getLastElement(SelectiveAccessState state) {
		return state.largeTuple.getLong(99);
	}

	/**
	 * Measures: compareTo() on two tuples with packed bytes.
	 *
	 * <p>New impl: unsigned byte array comparison.
	 * Old impl: element-by-element semantic comparison via IterableComparator
	 * (unless both had cached packed bytes).
	 */
	@Benchmark
	public int compareTo(TupleState state) {
		return state.preBuiltTuple.compareTo(state.preBuiltTupleCopy);
	}

	/**
	 * Measures: full lifecycle -- construct from list, pack, deserialize, access all
	 * elements via typed getters. Run with {@code -prof gc} to see total allocation.
	 */
	@Benchmark
	public void constructAndAccess(TupleState state, Blackhole bh) {
		Tuple t = Tuple.fromList(state.elements);
		byte[] packed = t.pack();
		Tuple t2 = Tuple.fromBytes(packed);
		for (int i = 0; i < state.tupleSize; i++) {
			bh.consume(t2.get(i));
		}
	}

	// -----------------------------------------------------------------------
	// Main (programmatic runner)
	// -----------------------------------------------------------------------

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(TupleBenchmark.class.getSimpleName())
				.warmupIterations(5)
				.measurementIterations(10)
				.forks(2)
				.build();
		new Runner(opt).run();
	}
}

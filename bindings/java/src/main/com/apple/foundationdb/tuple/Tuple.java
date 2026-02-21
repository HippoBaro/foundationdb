/*
 * Tuple.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.apple.foundationdb.Range;

/**
 * Represents a set of elements that make up a sortable, typed key. This object
 *  is comparable with other {@code Tuple}s and will sort in Java in
 *  the same order in which they would sort in FoundationDB. {@code Tuple}s sort
 *  first by the first element, then by the second, etc. This makes the tuple layer
 *  ideal for building a variety of higher-level data models.<br>
 * <h2>Types</h2>
 * A {@code Tuple} can
 *  contain byte arrays ({@code byte[]}), {@link String}s, {@link Number}s, {@link UUID}s,
 *  {@code boolean}s, {@link List}s, {@link Versionstamp}s, other {@code Tuple}s, and {@code null}.
 *  {@link Float} and {@link Double} instances will be serialized as single- and double-precision
 *  numbers respectively, and {@link BigInteger}s within the range [{@code -2^2040+1},
 *  {@code 2^2040-1}] are serialized without loss of precision (those outside the range
 *  will raise an {@link IllegalArgumentException}). All other {@code Number}s will be converted to
 *  a {@code long} integral value, so the range will be constrained to
 *  [{@code -2^63}, {@code 2^63-1}]. Note that for numbers outside this range the way that Java
 *  truncates integral values may yield unexpected results.<br>
 * <h2>{@code null} values</h2>
 * The FoundationDB tuple specification has a special type-code for {@code None}; {@code nil}; or,
 *  as Java would understand it, {@code null}.
 *  The behavior of the layer in the presence of {@code null} varies by type with the intention
 *  of matching expected behavior in Java. {@code byte[]}, {@link String}s, {@link UUID}s, and
 *  nested {@link List}s and {@code Tuple}s can be {@code null},
 *  whereas numbers (e.g., {@code long}s and {@code double}s) and booleans cannot.
 *  This means that the typed getters ({@link #getBytes(int) getBytes()}, {@link #getString(int) getString()}),
 *  {@link #getUUID(int) getUUID()}, {@link #getNestedTuple(int) getNestedTuple()}, {@link #getVersionstamp(int) getVersionstamp},
 *  and {@link #getNestedList(int) getNestedList()}) will return {@code null} if the entry at that location was
 *  {@code null} and the typed adds ({@link #add(byte[])}, {@link #add(String)}, {@link #add(Versionstamp)}
 *  {@link #add(Tuple)}, and {@link #add(List)}) will accept {@code null}. The
 *  {@link #getLong(int) typed get for integers} and other typed getters, however, will throw a
 *  {@link NullPointerException} if the entry in the {@code Tuple} was {@code null} at that position.<br>
 * <br>
 * This class is not thread safe.
 */
public class Tuple implements Comparable<Tuple>, Iterable<Object> {
	private static final byte[] EMPTY_BYTES = new byte[0];

	// The sole internal representation: always non-null after construction.
	private final byte[] packed;
	// Number of top-level elements. Always non-negative after construction.
	private final int elementCount;
	// Lazily-built index of byte offsets for each element within packed[].
	private int[] elementOffsets;
	// Lazily-materialized decoded elements. Populated on first call to materialize().
	private List<Object> materialized;
	// Memoized hash code (0 means not yet computed).
	private int memoizedHash = 0;
	// Whether this tuple contains at least one incomplete versionstamp.
	private final boolean incompleteVersionstamp;

	// -----------------------------------------------------------------------
	// Private constructors
	// -----------------------------------------------------------------------

	/**
	 * Core constructor: takes ownership of already-packed bytes.
	 */
	private Tuple(byte[] packed, int elementCount, boolean incompleteVersionstamp) {
		this.packed = packed;
		this.elementCount = elementCount;
		this.incompleteVersionstamp = incompleteVersionstamp;
	}

	/**
	 * Construct a new empty {@code Tuple}. After creation, items can be added
	 *  with calls to the variations of {@code add()}.
	 *
	 * @see #from(Object...)
	 * @see #fromBytes(byte[])
	 * @see #fromItems(Iterable)
	 */
	public Tuple() {
		this.packed = EMPTY_BYTES;
		this.elementCount = 0;
		this.elementOffsets = new int[0];
		this.incompleteVersionstamp = false;
	}

	// -----------------------------------------------------------------------
	// Internal helpers
	// -----------------------------------------------------------------------

	/**
	 * Packs a list of objects into a byte array. Returns the packed bytes.
	 */
	private static byte[] packObjects(List<?> items) {
		int size = 0;
		for(Object item : items) {
			size += TupleCodec.encodedObjectSize(item);
		}
		byte[] buf = new byte[size];
		int pos = 0;
		for(Object item : items) {
			pos += TupleCodec.encodeObject(buf, pos, item);
		}
		return (pos == size) ? buf : Arrays.copyOf(buf, pos);
	}

	/**
	 * Builds the element offset index by scanning packed bytes.
	 * Returns the offsets array.
	 */
	private int[] ensureOffsets() {
		if(elementOffsets != null) {
			return elementOffsets;
		}
		int[] offsets = new int[elementCount];
		int pos = 0;
		for(int i = 0; i < elementCount; i++) {
			offsets[i] = pos;
			pos += TupleCodec.elementSize(packed, pos);
		}
		elementOffsets = offsets;
		return offsets;
	}

	/**
	 * Creates a new Tuple by encoding a single object and appending it to this tuple's packed bytes.
	 */
	private Tuple appendObject(Object item, int exactSize, boolean hasIncomplete) {
		byte[] newPacked = new byte[this.packed.length + exactSize];
		System.arraycopy(this.packed, 0, newPacked, 0, this.packed.length);
		TupleCodec.encodeObject(newPacked, this.packed.length, item);
		return new Tuple(newPacked, this.elementCount + 1,
				this.incompleteVersionstamp || hasIncomplete);
	}

	/**
	 * Materializes all elements from packed bytes into a list, caching the result.
	 */
	private List<Object> materialize() {
		if(materialized != null) {
			return materialized;
		}
		if(packed.length == 0) {
			return materialized = new ArrayList<>();
		}
		int count = size();
		List<Object> items = new ArrayList<>(count);
		int[] offsets = ensureOffsets();
		for(int i = 0; i < count; i++) {
			items.add(TupleCodec.decodeObject(packed, offsets[i]));
		}
		return materialized = items;
	}


	// -----------------------------------------------------------------------
	// add methods (immutable - each returns a new Tuple)
	// -----------------------------------------------------------------------

	/**
	 * Creates a copy of this {@code Tuple} with an appended last element. The
	 * parameter is untyped but only {@link String}, {@code byte[]},
	 * {@link Number}s, {@link UUID}s, {@link Boolean}s, {@link List}s,
	 * {@code Tuple}s, and {@code null} are allowed. If an object of another type is
	 * passed, then an {@link IllegalArgumentException} is thrown.
	 *
	 * @param o the object to append. Must be {@link String}, {@code byte[]},
	 *          {@link Number}s, {@link UUID}, {@link List}, {@link Boolean}, or
	 *          {@code null}.
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple addObject(Object o) {
		if(o instanceof String) {
			return add((String) o);
		}
		else if(o instanceof byte[]) {
			return add((byte[]) o);
		}
		else if(o instanceof UUID) {
			return add((UUID) o);
		}
		else if(o instanceof List<?>) {
			return add((List<?>) o);
		}
		else if(o instanceof Tuple) {
			return add((Tuple) o);
		}
		else if(o instanceof Boolean) {
			return add((Boolean) o);
		}
		else if(o instanceof Versionstamp) {
			return add((Versionstamp) o);
		}
		else if(o instanceof BigInteger) {
			return add((BigInteger) o);
		}
		else if(o instanceof Float) {
			return add((float)(Float) o);
		}
		else if(o instanceof Double) {
			return add((double)(Double) o);
		}
		else if(o instanceof Number) {
			return add(((Number) o).longValue());
		}
		else if(o == null) {
			return appendObject(null, 1, false);
		}
		else {
			throw new IllegalArgumentException("Parameter type (" + o.getClass().getName() + ") not recognized");
		}
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@code String} appended as the last element.
	 *
	 * @param s the {@code String} to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(String s) {
		if(s == null) {
			return appendObject(null, 1, false);
		}
		StringUtil.validate(s);
		return appendObject(s, 2 + StringUtil.packedSize(s), false);
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@code long} appended as the last element.
	 *
	 * @param l the number to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(long l) {
		return appendObject(l, TupleCodec.encodedLongSize(l), false);
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@code byte} array appended as the last element.
	 *
	 * @param b the {@code byte}s to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(byte[] b) {
		if(b == null) {
			return appendObject(null, 1, false);
		}
		return appendObject(b, 2 + b.length + ByteArrayUtil.nullCount(b), false);
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@code boolean} appended as the last element.
	 *
	 * @param b the {@code boolean} to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(boolean b) {
		return appendObject(b, 1, false);
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@link UUID} appended as the last element.
	 *
	 * @param uuid the {@link UUID} to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(UUID uuid) {
		if(uuid == null) {
			return appendObject(null, 1, false);
		}
		return appendObject(uuid, 17, false);
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@link BigInteger} appended as the last element.
	 *  As {@link Tuple}s cannot contain {@code null} numeric types, a {@link NullPointerException}
	 *  is raised if a {@code null} argument is passed.
	 *
	 * @param bi the {@link BigInteger} to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(BigInteger bi) {
		if(bi == null) {
			throw new NullPointerException("Number types in Tuple cannot be null");
		}
		return appendObject(bi, TupleCodec.encodedBigIntegerSize(bi), false);
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@code float} appended as the last element.
	 *
	 * @param f the {@code float} to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(float f) {
		return appendObject(f, 5, false);
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@code double} appended as the last element.
	 *
	 * @param d the {@code double} to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(double d) {
		return appendObject(d, 9, false);
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@link Versionstamp} object appended as the last
	 *  element.
	 *
	 * @param v the {@link Versionstamp} to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(Versionstamp v) {
		if(v == null) {
			return appendObject(null, 1, false);
		}
		return appendObject(v, 1 + Versionstamp.LENGTH, !v.isComplete());
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@link List} appended as the last element.
	 *  This does not add the elements individually (for that, use {@link Tuple#addAll(List) Tuple.addAll}).
	 *  This adds the list as a single element nested within the outer {@code Tuple}.
	 *
	 * @param l the {@link List} to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(List<?> l) {
		if(l == null) {
			return appendObject(null, 1, false);
		}
		TupleCodec.validateStrings(l);
		return appendObject(l, TupleCodec.encodedObjectSize(l),
				TupleCodec.hasIncompleteVersionstamp(l));
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@code Tuple} appended as the last element.
	 *  This does not add the elements individually (for that, use {@link Tuple#addAll(Tuple) Tuple.addAll}).
	 *  This adds the list as a single element nested within the outer {@code Tuple}.
	 *
	 * @param t the {@code Tuple} to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(Tuple t) {
		if(t == null) {
			return appendObject(null, 1, false);
		}
		return appendObject(t, TupleCodec.encodedObjectSize(t), t.hasIncompleteVersionstamp());
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@code byte} array appended as the last element.
	 *
	 * @param b the {@code byte}s to append
	 * @param offset the starting index of {@code b} to add
	 * @param length the number of elements of {@code b} to copy into this {@code Tuple}
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(byte[] b, int offset, int length) {
		return add(Arrays.copyOfRange(b, offset, offset + length));
	}

	/**
	 * Create a copy of this {@code Tuple} with a list of items appended.
	 *
	 * @param o the list of objects to append. Elements must be {@link String}, {@code byte[]},
	 *  {@link Number}s, or {@code null}.
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple addAll(List<?> o) {
		TupleCodec.validateStrings(o);
		byte[] otherPacked = packObjects(o);
		byte[] newPacked = new byte[this.packed.length + otherPacked.length];
		System.arraycopy(this.packed, 0, newPacked, 0, this.packed.length);
		System.arraycopy(otherPacked, 0, newPacked, this.packed.length, otherPacked.length);
		boolean otherHasIncomplete = TupleCodec.hasIncompleteVersionstamp(o);
		return new Tuple(newPacked, this.elementCount + o.size(),
				this.incompleteVersionstamp || otherHasIncomplete);
	}

	/**
	 * Create a copy of this {@code Tuple} with all elements from anther {@code Tuple} appended.
	 *
	 * @param other the {@code Tuple} whose elements should be appended
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple addAll(Tuple other) {
		byte[] newPacked = new byte[this.packed.length + other.packed.length];
		System.arraycopy(this.packed, 0, newPacked, 0, this.packed.length);
		System.arraycopy(other.packed, 0, newPacked, this.packed.length, other.packed.length);
		return new Tuple(newPacked, this.elementCount + other.size(),
				this.incompleteVersionstamp || other.incompleteVersionstamp);
	}

	// -----------------------------------------------------------------------
	// pack / packInto / packWithVersionstamp
	// -----------------------------------------------------------------------

	/**
	 * Get an encoded representation of this {@code Tuple}. Each element is encoded to
	 *  {@code byte}s and concatenated. Note that once a {@code Tuple} has been packed, its
	 *  serialized representation is stored internally so that future calls to this function
	 *  are faster than the initial call.
	 *
	 * @return a packed representation of this {@code Tuple}
	 */
	public byte[] pack() {
		return packInternal(null, true);
	}

	/**
	 * Get an encoded representation of this {@code Tuple}. Each element is encoded to
	 *  {@code byte}s and concatenated, and then the prefix supplied is prepended to
	 *  the array. Note that once a {@code Tuple} has been packed, its serialized representation
	 *  is stored internally so that future calls to this function are faster than the
	 *  initial call.
	 *
	 * @param prefix additional byte-array prefix to prepend to the packed bytes
	 * @return a packed representation of this {@code Tuple} prepended by the {@code prefix}
	 */
	public byte[] pack(byte[] prefix) {
		return packInternal(prefix, true);
	}

	byte[] packInternal(byte[] prefix, boolean copy) {
		if(hasIncompleteVersionstamp()) {
			throw new IllegalArgumentException("Incomplete Versionstamp included in vanilla tuple pack");
		}
		boolean hasPrefix = prefix != null && prefix.length > 0;
		if(hasPrefix) {
			return ByteArrayUtil.join(prefix, packed);
		}
		else if(copy) {
			return Arrays.copyOf(packed, packed.length);
		}
		else {
			return packed;
		}
	}

	/**
	 * Pack an encoded representation of this {@code Tuple} onto the end of the given {@link ByteBuffer}.
	 *  It is up to the caller to ensure that there is enough space allocated within the buffer
	 *  to avoid {@link java.nio.BufferOverflowException}s. The client may call {@link #getPackedSize()}
	 *  to determine how large this {@code Tuple} will be once packed in order to allocate sufficient memory.
	 *  Note that unlike {@link #pack()}, the serialized representation of this {@code Tuple} is not stored, so
	 *  calling this function multiple times with the same {@code Tuple} requires serializing the {@code Tuple}
	 *  multiple times.
	 * <br>
	 * <br>
	 * This method will throw an error if there are any incomplete {@link Versionstamp}s in this {@code Tuple}.
	 *
	 * @param dest the destination {@link ByteBuffer} for the encoded {@code Tuple}
	 */
	public void packInto(ByteBuffer dest) {
		if(hasIncompleteVersionstamp()) {
			throw new IllegalArgumentException("Incomplete Versionstamp included in vanilla tuple pack");
		}
		dest.put(packed);
	}

	/**
	 * Get an encoded representation of this {@code Tuple} for use with
	 *  {@link com.apple.foundationdb.MutationType#SET_VERSIONSTAMPED_KEY MutationType.SET_VERSIONSTAMPED_KEY}.
	 *  This works the same as the {@link #packWithVersionstamp(byte[]) one-paramter version of this method},
	 *  but it does not add any prefix to the array.
	 *
	 * @return a packed representation of this {@code Tuple} for use with versionstamp ops.
	 * @throws IllegalArgumentException if there is not exactly one incomplete {@link Versionstamp} included in this {@code Tuple}
	 */
	public byte[] packWithVersionstamp() {
		return packWithVersionstamp(null);
	}

	/**
	 * Get an encoded representation of this {@code Tuple} for use with
	 *  {@link com.apple.foundationdb.MutationType#SET_VERSIONSTAMPED_KEY MutationType.SET_VERSIONSTAMPED_KEY}.
	 *  There must be exactly one incomplete {@link Versionstamp} instance within this
	 *  {@code Tuple} or this will throw an {@link IllegalArgumentException}.
	 *  Each element is encoded to {@code byte}s and concatenated, the prefix
	 *  is then prepended to the array, and then the index of the packed incomplete
	 *  {@link Versionstamp} is appended as a little-endian integer. This can then be passed
	 *  as the key to
	 *  {@link com.apple.foundationdb.Transaction#mutate(com.apple.foundationdb.MutationType, byte[], byte[]) Transaction.mutate()}
	 *  with the {@code SET_VERSIONSTAMPED_KEY} {@link com.apple.foundationdb.MutationType}, and the transaction's
	 *  version will then be filled in at commit time.
	 * <br>
	 * <br>
	 * Note that once a {@code Tuple} has been packed, its serialized representation is stored internally so that
	 *  future calls to this function are faster than the initial call.
	 *
	 * @param prefix additional byte-array prefix to prepend to packed bytes.
	 * @return a packed representation of this {@code Tuple} for use with versionstamp ops.
	 * @throws IllegalArgumentException if there is not exactly one incomplete {@link Versionstamp} included in this {@code Tuple}
	 */
	public byte[] packWithVersionstamp(byte[] prefix) {
		return packWithVersionstampInternal(prefix, true);
	}

	byte[] packWithVersionstampInternal(byte[] prefix, boolean copy) {
		if(!hasIncompleteVersionstamp()) {
			throw new IllegalArgumentException("No incomplete Versionstamp included in tuple pack with versionstamp");
		}

		// Single scan: count incomplete versionstamps and find offset of the first
		int[] vsInfo = TupleCodec.scanIncompleteVersionstampInfo(packed, 0, packed.length);
		int incompleteCount = vsInfo[0];
		int versionPos = vsInfo[1];

		if(incompleteCount > 1) {
			throw new IllegalArgumentException("Tuple contains more than one incomplete Versionstamp");
		}
		if(versionPos < 0) {
			throw new IllegalArgumentException("No incomplete Versionstamp found in packed tuple data");
		}

		boolean hasPrefix = prefix != null && prefix.length > 0;
		int prefixLen = hasPrefix ? prefix.length : 0;
		int adjustedVersionPos = versionPos + prefixLen;

		boolean useOldFormat = TupleCodec.useOldVersionOffsetFormat();
		int suffixSize = useOldFormat ? Short.BYTES : Integer.BYTES;

		if(useOldFormat && adjustedVersionPos > 0xffff) {
			throw new IllegalArgumentException("Tuple has incomplete version at position " + adjustedVersionPos +
					" which is greater than the maximum " + 0xffff);
		}

		byte[] result = new byte[prefixLen + packed.length + suffixSize];
		int pos = 0;
		if(hasPrefix) {
			System.arraycopy(prefix, 0, result, 0, prefixLen);
			pos = prefixLen;
		}
		System.arraycopy(packed, 0, result, pos, packed.length);
		pos += packed.length;

		// Append version position suffix in little-endian
		if(useOldFormat) {
			result[pos]     = (byte)(adjustedVersionPos & 0xFF);
			result[pos + 1] = (byte)((adjustedVersionPos >> 8) & 0xFF);
		}
		else {
			result[pos]     = (byte)(adjustedVersionPos & 0xFF);
			result[pos + 1] = (byte)((adjustedVersionPos >> 8) & 0xFF);
			result[pos + 2] = (byte)((adjustedVersionPos >> 16) & 0xFF);
			result[pos + 3] = (byte)((adjustedVersionPos >> 24) & 0xFF);
		}

		return result;
	}

	byte[] packMaybeVersionstamp() {
		if(hasIncompleteVersionstamp()) {
			return packWithVersionstampInternal(null, false);
		}
		else {
			return packed;
		}
	}

	/**
	 * Returns the vanilla packed bytes for this tuple (without versionstamp suffix).
	 * Package-private, for use by TupleCodec.
	 */
	byte[] getPackedBytes() {
		return packed;
	}

	// -----------------------------------------------------------------------
	// getItems / getRawItems / stream / iterator
	// -----------------------------------------------------------------------

	/**
	 * Gets the unserialized contents of this {@code Tuple}.
	 *
	 * @return the elements that make up this {@code Tuple}.
	 */
	public List<Object> getItems() {
		return materialize();
	}

	/**
	 * Gets a {@link Stream} of the unserialized contents of this {@code Tuple}.
	 *
	 * @return a {@link Stream} of the elements that make up this {@code Tuple}.
	 */
	public Stream<Object> stream() {
		return materialize().stream();
	}

	/**
	 * Gets an {@code Iterator} over the {@code Objects} in this {@code Tuple}. This {@code Iterator} is
	 *  unmodifiable and will throw an exception if {@link Iterator#remove() remove()} is called.
	 *
	 * @return an unmodifiable {@code Iterator} over the elements in the {@code Tuple}.
	 */
	@Override
	public Iterator<Object> iterator() {
		return Collections.unmodifiableList(materialize()).iterator();
	}

	// -----------------------------------------------------------------------
	// fromBytes / fromItems / fromList / fromStream / from
	// -----------------------------------------------------------------------

	/**
	 * Construct a new {@code Tuple} with elements decoded from a supplied {@code byte} array.
	 *  The passed byte array must not be {@code null}. This will throw an exception if the passed byte
	 *  array does not represent a valid {@code Tuple}. For example, this will throw an error if it
	 *  encounters an unknown type code or if there is a packed element that appears to be truncated.
	 *
	 * @param bytes encoded {@code Tuple} source
	 *
	 * @return a new {@code Tuple} constructed by deserializing the provided {@code byte} array
	 * @throws IllegalArgumentException if {@code bytes} does not represent a valid {@code Tuple}
	 */
	public static Tuple fromBytes(byte[] bytes) {
		return fromBytes(bytes, 0, bytes.length);
	}

	/**
	 * Construct a new {@code Tuple} with elements decoded from a supplied {@code byte} array.
	 *  The passed byte array must not be {@code null}. This will throw an exception if the specified slice of
	 *  the passed byte array does not represent a valid {@code Tuple}. For example, this will throw an error
	 *  if it encounters an unknown type code or if there is a packed element that appears to be truncated.
	 *
	 * @param bytes encoded {@code Tuple} source
	 * @param offset starting offset of byte array of encoded data
	 * @param length length of encoded data within the source
	 *
	 * @return a new {@code Tuple} constructed by deserializing the specified slice of the provided {@code byte} array
	 * @throws IllegalArgumentException if {@code offset} or {@code length} are negative or would exceed the size of
	 *  the array or if {@code bytes} does not represent a valid {@code Tuple}
	 */
	public static Tuple fromBytes(byte[] bytes, int offset, int length) {
		if(offset < 0 || offset > bytes.length) {
			throw new IllegalArgumentException("Invalid offset for Tuple deserialization");
		}
		if(length < 0 || offset + length > bytes.length) {
			throw new IllegalArgumentException("Invalid length for Tuple deserialization");
		}
		byte[] p = Arrays.copyOfRange(bytes, offset, offset + length);
		TupleCodec.ScanResult result = TupleCodec.structuralCountAndScan(p, 0, p.length);
		Tuple t = new Tuple(p, result.offsets.length, result.hasIncompleteVersionstamp);
		t.elementOffsets = result.offsets;
		return t;
	}

	/**
	 * Wraps an already-packed {@code byte} array as a {@code Tuple} without copying or validating.
	 *  The caller asserts that {@code bytes} is a valid packed tuple encoding exactly
	 *  {@code elementCount} top-level elements, and that the array will not be mutated
	 *  after this call. No structural validation, element counting, or defensive copy is
	 *  performed. Passing invalid data will result in undefined behavior on subsequent
	 *  access (e.g., {@link ArrayIndexOutOfBoundsException} or corrupt decoded values).
	 *
	 * <p>This is intended for callers that have already validated the data or that receive
	 *  it from a trusted source (e.g., the native FDB client).
	 *
	 * @param bytes a valid packed tuple encoding; ownership is transferred to the returned {@code Tuple}
	 * @param elementCount the number of top-level elements encoded in {@code bytes}
	 *
	 * @return a new {@code Tuple} backed by the provided byte array
	 */
	public static Tuple wrap(byte[] bytes, int elementCount) {
		return new Tuple(bytes, elementCount, false);
	}

	/**
	 * Gets the number of elements in this {@code Tuple}.
	 *
	 * @return the number of elements in this {@code Tuple}
	 */
	public int size() {
		return elementCount;
	}

	/**
	 * Determine if this {@code Tuple} contains no elements.
	 *
	 * @return {@code true} if this {@code Tuple} contains no elements, {@code false} otherwise
	 */
	public boolean isEmpty() {
		return packed.length == 0;
	}

	// -----------------------------------------------------------------------
	// Typed getters - decode directly from packed bytes
	// -----------------------------------------------------------------------

	/**
	 * Gets an indexed item as a {@code long}. This function will not do type conversion
	 *  and so will throw a {@code ClassCastException} if the element is not a number type.
	 *  The element at the index may not be {@code null}.
	 *
	 * @param index the location of the item to return
	 *
	 * @return the item at {@code index} as a {@code long}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@link Number}
	 * @throws NullPointerException if the element at {@code index} is {@code null}
	 */
	public long getLong(int index) {
		int[] offsets = ensureOffsets();
		int pos = offsets[index];
		int code = packed[pos] & 0xFF;
		if(code == TupleCodec.NULL_CODE) {
			throw new NullPointerException("Number types in Tuples may not be null");
		}
		if(code == TupleCodec.FLOAT_CODE) {
			return (long)TupleCodec.decodeFloat(packed, pos);
		}
		if(code == TupleCodec.DOUBLE_CODE) {
			return (long)TupleCodec.decodeDouble(packed, pos);
		}
		if(code == TupleCodec.POS_INT_END || code == TupleCodec.NEG_INT_START) {
			return TupleCodec.decodeBigInteger(packed, pos).longValue();
		}
		if((code > TupleCodec.NEG_INT_START && code < TupleCodec.POS_INT_END) || code == TupleCodec.INT_ZERO_CODE) {
			return TupleCodec.decodeLong(packed, pos);
		}
		// If it's not a numeric type, fall back to decodeObject and cast (will throw ClassCastException)
		Object o = TupleCodec.decodeObject(packed, pos);
		return ((Number)o).longValue();
	}

	/**
	 * Gets an indexed item as a {@code byte[]}. This function will not do type conversion
	 *  and so will throw a {@code ClassCastException} if the tuple element is not a
	 *  {@code byte} array.
	 *
	 * @param index the location of the element to return
	 *
	 * @return the item at {@code index} as a {@code byte[]}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@link Number}
	 */
	public byte[] getBytes(int index) {
		int[] offsets = ensureOffsets();
		int pos = offsets[index];
		int code = packed[pos] & 0xFF;
		if(code == TupleCodec.NULL_CODE) {
			return null;
		}
		if(code == TupleCodec.BYTES_CODE) {
			return TupleCodec.decodeBytes(packed, pos);
		}
		// Fall through to decodeObject for ClassCastException behavior
		Object o = TupleCodec.decodeObject(packed, pos);
		return (byte[])o;
	}

	/**
	 * Gets an indexed item as a {@code String}. This function will not do type conversion
	 *  and so will throw a {@code ClassCastException} if the tuple element is not of
	 *  {@code String} type.
	 *
	 * @param index the location of the element to return
	 *
	 * @return the item at {@code index} as a {@code String}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@link String}
	 */
	public String getString(int index) {
		int[] offsets = ensureOffsets();
		int pos = offsets[index];
		int code = packed[pos] & 0xFF;
		if(code == TupleCodec.NULL_CODE) {
			return null;
		}
		if(code == TupleCodec.STRING_CODE) {
			return TupleCodec.decodeString(packed, pos);
		}
		// Fall through to decodeObject for ClassCastException behavior
		Object o = TupleCodec.decodeObject(packed, pos);
		return (String)o;
	}

	/**
	 * Gets an indexed item as a {@link BigInteger}. This function will not do type conversion
	 *  and so will throw a {@code ClassCastException} if the tuple element is not of
	 *  a {@code Number} type. If the underlying type is a floating point value, this
	 *  will lead to a loss of precision. The element at the index may not be {@code null}.
	 *
	 * @param index the location of the element to return
	 *
	 * @return the item at {@code index} as a {@link BigInteger}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@link Number}
	 */
	public BigInteger getBigInteger(int index) {
		int[] offsets = ensureOffsets();
		int pos = offsets[index];
		int code = packed[pos] & 0xFF;
		if(code == TupleCodec.NULL_CODE) {
			throw new NullPointerException("Number types in Tuples may not be null");
		}
		if(code == TupleCodec.POS_INT_END || code == TupleCodec.NEG_INT_START) {
			return TupleCodec.decodeBigInteger(packed, pos);
		}
		if(code > TupleCodec.NEG_INT_START && code < TupleCodec.POS_INT_END) {
			// Regular integer range - may decode to long or BigInteger
			Object decoded = TupleCodec.decodeObject(packed, pos);
			if(decoded instanceof BigInteger) {
				return (BigInteger)decoded;
			}
			return BigInteger.valueOf(((Number)decoded).longValue());
		}
		if(code == TupleCodec.FLOAT_CODE) {
			return BigInteger.valueOf((long)TupleCodec.decodeFloat(packed, pos));
		}
		if(code == TupleCodec.DOUBLE_CODE) {
			return BigInteger.valueOf((long)TupleCodec.decodeDouble(packed, pos));
		}
		// Fall back for ClassCastException
		Object o = TupleCodec.decodeObject(packed, pos);
		if(o instanceof BigInteger) {
			return (BigInteger)o;
		}
		return BigInteger.valueOf(((Number)o).longValue());
	}

	/**
	 * Gets an indexed item as a {@code float}. This function will not do type conversion
	 *  and so will throw a {@code ClassCastException} if the element is not a number type.
	 *  The element at the index may not be {@code null}.
	 *
	 * @param index the location of the item to return
	 *
	 * @return the item at {@code index} as a {@code float}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@link Number}
	 */
	public float getFloat(int index) {
		int[] offsets = ensureOffsets();
		int pos = offsets[index];
		int code = packed[pos] & 0xFF;
		if(code == TupleCodec.NULL_CODE) {
			throw new NullPointerException("Number types in Tuples may not be null");
		}
		if(code == TupleCodec.FLOAT_CODE) {
			return TupleCodec.decodeFloat(packed, pos);
		}
		if(code == TupleCodec.DOUBLE_CODE) {
			return (float)TupleCodec.decodeDouble(packed, pos);
		}
		// For integer types, decode and convert
		Object o = TupleCodec.decodeObject(packed, pos);
		return ((Number)o).floatValue();
	}

	/**
	 * Gets an indexed item as a {@code double}. This function will not do type conversion
	 *  and so will throw a {@code ClassCastException} if the element is not a number type.
	 *  The element at the index may not be {@code null}.
	 *
	 * @param index the location of the item to return
	 *
	 * @return the item at {@code index} as a {@code double}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@link Number}
	 */
	public double getDouble(int index) {
		int[] offsets = ensureOffsets();
		int pos = offsets[index];
		int code = packed[pos] & 0xFF;
		if(code == TupleCodec.NULL_CODE) {
			throw new NullPointerException("Number types in Tuples may not be null");
		}
		if(code == TupleCodec.DOUBLE_CODE) {
			return TupleCodec.decodeDouble(packed, pos);
		}
		if(code == TupleCodec.FLOAT_CODE) {
			return (double)TupleCodec.decodeFloat(packed, pos);
		}
		// For integer types, decode and convert
		Object o = TupleCodec.decodeObject(packed, pos);
		return ((Number)o).doubleValue();
	}

	/**
	 * Gets an indexed item as a {@code boolean}. This function will not do type conversion
	 *  and so will throw a {@code ClassCastException} if the element is not a {@code Boolean}.
	 *  The element at the index may not be {@code null}.
	 *
	 * @param index the location of the item to return
	 *
	 * @return the item at {@code index} as a {@code boolean}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@link Boolean}
	 * @throws NullPointerException if the element at {@code index} is {@code null}
	 */
	public boolean getBoolean(int index) {
		int[] offsets = ensureOffsets();
		int pos = offsets[index];
		int code = packed[pos] & 0xFF;
		if(code == TupleCodec.NULL_CODE) {
			throw new NullPointerException("Boolean type in Tuples may not be null");
		}
		if(code == TupleCodec.TRUE_CODE || code == TupleCodec.FALSE_CODE) {
			return TupleCodec.decodeBoolean(packed, pos);
		}
		// Fall through for ClassCastException
		Object o = TupleCodec.decodeObject(packed, pos);
		return (Boolean)o;
	}

	/**
	 * Gets an indexed item as a {@link UUID}. This function will not do type conversion
	 *  and so will throw a {@code ClassCastException} if the element is not a {@code UUID}.
	 *  The element at the index may be {@code null}.
	 *
	 * @param index the location of the item to return
	 *
	 * @return the item at {@code index} as a {@link UUID}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@link UUID}
	 */
	public UUID getUUID(int index) {
		int[] offsets = ensureOffsets();
		int pos = offsets[index];
		int code = packed[pos] & 0xFF;
		if(code == TupleCodec.NULL_CODE) {
			return null;
		}
		if(code == TupleCodec.UUID_CODE) {
			return TupleCodec.decodeUUID(packed, pos);
		}
		// Fall through for ClassCastException
		Object o = TupleCodec.decodeObject(packed, pos);
		return (UUID)o;
	}

	/**
	 * Gets an indexed item as a {@link Versionstamp}. This function will not do type
	 *  conversion and so will throw a {@code ClassCastException} if the element is not
	 *  a {@code Versionstamp}. The element at the index may be {@code null}.
	 *
	 * @param index the location of the item to return
	 *
	 * @return the item at {@code index} as a {@link Versionstamp}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@link Versionstamp}
	 */
	public Versionstamp getVersionstamp(int index) {
		int[] offsets = ensureOffsets();
		int pos = offsets[index];
		int code = packed[pos] & 0xFF;
		if(code == TupleCodec.NULL_CODE) {
			return null;
		}
		if(code == TupleCodec.VERSIONSTAMP_CODE) {
			return TupleCodec.decodeVersionstamp(packed, pos);
		}
		// Fall through for ClassCastException
		Object o = TupleCodec.decodeObject(packed, pos);
		return (Versionstamp)o;
	}

	/**
	 * Gets an indexed item as a {@link List}. This function will not do type conversion
	 *  and so will throw a {@code ClassCastException} if the element is not a {@link List}
	 *  or {@code Tuple}. The element at the index may be {@code null}.
	 *
	 * @param index the location of the item to return
	 *
	 * @return the item at {@code index} as a {@link List}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@link List}
	 *         or a {@code Tuple}
	 */
	public List<Object> getNestedList(int index) {
		int[] offsets = ensureOffsets();
		int pos = offsets[index];
		int code = packed[pos] & 0xFF;
		if(code == TupleCodec.NULL_CODE) {
			return null;
		}
		Object o = TupleCodec.decodeObject(packed, pos);
		if(o instanceof Tuple) {
			return ((Tuple)o).getItems();
		}
		else if(o instanceof List<?>) {
			@SuppressWarnings("unchecked")
			List<Object> list = new ArrayList<>((List<?>) o);
			return list;
		}
		else {
			throw new ClassCastException("Cannot convert item of type " + o.getClass() + " to list");
		}
	}

	/**
	 * Gets an indexed item as a {@link Tuple}. This function will not do type conversion
	 *  and so will throw a {@code ClassCastException} if the element is not a {@link List}
	 *  or {@code Tuple}. The element at the index may be {@code null}.
	 *
	 * @param index the location of the item to return
	 *
	 * @return the item at {@code index} as a {@link List}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@code Tuple}
	 *         or a {@link List}
	 */
	public Tuple getNestedTuple(int index) {
		int[] offsets = ensureOffsets();
		int pos = offsets[index];
		int code = packed[pos] & 0xFF;
		if(code == TupleCodec.NULL_CODE) {
			return null;
		}
		Object o = TupleCodec.decodeObject(packed, pos);
		if(o instanceof Tuple) {
			return (Tuple)o;
		}
		else if(o instanceof List<?>) {
			return Tuple.fromList((List<?>)o);
		}
		else {
			throw new ClassCastException("Cannot convert item of type " + o.getClass() + " to tuple");
		}
	}

	/**
	 * Gets an indexed item without forcing a type.
	 *
	 * @param index the index of the item to return
	 *
	 * @return an item from the list, without forcing type conversion
	 */
	public Object get(int index) {
		if(materialized != null) {
			return materialized.get(index);
		}
		int[] offsets = ensureOffsets();
		return TupleCodec.decodeObject(packed, offsets[index]);
	}

	// -----------------------------------------------------------------------
	// popFront / popBack
	// -----------------------------------------------------------------------

	/**
	 * Creates a new {@code Tuple} with the first item of this {@code Tuple} removed.
	 *
	 * @return a newly created {@code Tuple} without the first item of this {@code Tuple}
	 *
	 * @throws IllegalStateException if this {@code Tuple} is empty
	 */
	public Tuple popFront() {
		if(packed.length == 0)
			throw new IllegalStateException("Tuple contains no elements");

		int[] offsets = ensureOffsets();
		int firstSize = (offsets.length > 1) ? offsets[1] : packed.length;
		byte[] newPacked = Arrays.copyOfRange(packed, firstSize, packed.length);
		int newCount = elementCount - 1;
		// Only re-scan if the original had incomplete versionstamps
		boolean newHasIncomplete = incompleteVersionstamp && (newPacked.length > 0) &&
				TupleCodec.scanIncompleteVersionstampInfo(newPacked, 0, newPacked.length)[0] > 0;
		return new Tuple(newPacked, newCount, newHasIncomplete);
	}

	/**
	 * Creates a new {@code Tuple} with the last item of this {@code Tuple} removed.
	 *
	 * @return a newly created {@code Tuple} without the last item of this {@code Tuple}
	 *
	 * @throws IllegalStateException if this {@code Tuple} is empty
	 */
	public Tuple popBack() {
		if(packed.length == 0)
			throw new IllegalStateException("Tuple contains no elements");

		int[] offsets = ensureOffsets();
		int lastStart = offsets[offsets.length - 1];
		byte[] newPacked = Arrays.copyOf(packed, lastStart);
		int newCount = elementCount - 1;
		// Only re-scan if the original had incomplete versionstamps
		boolean newHasIncomplete = incompleteVersionstamp && (newPacked.length > 0) &&
				TupleCodec.scanIncompleteVersionstampInfo(newPacked, 0, newPacked.length)[0] > 0;
		return new Tuple(newPacked, newCount, newHasIncomplete);
	}

	// -----------------------------------------------------------------------
	// range
	// -----------------------------------------------------------------------

	/**
	 * Returns a range representing all keys that encode {@code Tuple}s strictly starting
	 *  with this {@code Tuple}.
	 * <br>
	 * <br>
	 * For example:
	 * <pre>
	 *   Tuple t = Tuple.from("a", "b");
	 *   Range r = t.range();</pre>
	 * {@code r} includes all tuples ("a", "b", ...)
	 * <br>
	 * This function will throw an error if this {@code Tuple} contains an incomplete
	 *  {@link Versionstamp}.
	 *
	 * @return the range of keys containing all possible keys that have this {@code Tuple}
	 *  as a strict prefix
	 */
	public Range range() {
		return range(null);
	}

	/**
	 * Returns a range representing all keys that encode {@code Tuple}s strictly starting
	 *  with the given prefix followed by this {@code Tuple}.
	 * <br>
	 * <br>
	 * For example:
	 * <pre>
	 *   Tuple t = Tuple.from("a", "b");
	 *   Range r = t.range(Tuple.from("c").pack());</pre>
	 * {@code r} contains all tuples ("c", "a", "b", ...)
	 * <br>
	 * This function will throw an error if this {@code Tuple} contains an incomplete
	 *  {@link Versionstamp}.
	 *
	 * @param prefix a byte prefix to precede all elements in the range
	 *
	 * @return the range of keys containing all possible keys that have {@code prefix}
	 *   followed by this {@code Tuple} as a strict prefix
	 */
	public Range range(byte[] prefix) {
		if(hasIncompleteVersionstamp()) {
			throw new IllegalStateException("Tuple with incomplete versionstamp used for range");
		}
		byte[] p = packInternal(prefix, false);
		return new Range(ByteArrayUtil.join(p, new byte[] {0x0}),
				ByteArrayUtil.join(p, new byte[] {(byte)0xff}));
	}

	// -----------------------------------------------------------------------
	// hasIncompleteVersionstamp / getPackedSize
	// -----------------------------------------------------------------------

	/**
	 * Determines if there is a {@link Versionstamp} included in this {@code Tuple} that has
	 *  not had its transaction version set. It will search through nested {@code Tuple}s
	 *  contained within this {@code Tuple}. It will not throw an error if it finds multiple
	 *  incomplete {@code Versionstamp} instances.
	 *
	 * @return whether there is at least one incomplete {@link Versionstamp} included in this
	 *  {@code Tuple}
	 */
	public boolean hasIncompleteVersionstamp() {
		return incompleteVersionstamp;
	}

	/**
	 * Get the number of bytes in the packed representation of this {@code Tuple}. This is done by summing
	 *  the serialized sizes of all of the elements of this {@code Tuple} and does not pack everything
	 *  into a single {@code Tuple}. The return value of this function is stored within this {@code Tuple}
	 *  after this function has been called so that subsequent calls on the same object are fast. This method
	 *  does not validate that there is not more than one incomplete {@link Versionstamp} in this {@code Tuple}.
	 *
	 * @return the number of bytes in the packed representation of this {@code Tuple}
	 */
	public int getPackedSize() {
		if(incompleteVersionstamp) {
			int suffixSize = TupleCodec.useOldVersionOffsetFormat() ? Short.BYTES : Integer.BYTES;
			int incompleteCount = TupleCodec.scanIncompleteVersionstampInfo(packed, 0, packed.length)[0];
			return packed.length + incompleteCount * suffixSize;
		}
		return packed.length;
	}

	// -----------------------------------------------------------------------
	// compareTo / hashCode / equals / toString
	// -----------------------------------------------------------------------

	/**
	 * Compare the byte-array representation of this {@code Tuple} against another. This method
	 *  will sort {@code Tuple}s in the same order that they would be sorted as keys in
	 *  FoundationDB. Returns a negative integer, zero, or a positive integer when this object's
	 *  byte-array representation is found to be less than, equal to, or greater than the
	 *  specified {@code Tuple}.
	 *
	 * @param t the {@code Tuple} against which to compare
	 *
	 * @return a negative integer, zero, or a positive integer when this {@code Tuple} is
	 *  less than, equal, or greater than the parameter {@code t}.
	 */
	@Override
	public int compareTo(Tuple t) {
		// If either tuple has an incomplete versionstamp, then there is a possibility that the byte order
		// is not the semantic comparison order.
		if(!hasIncompleteVersionstamp() && !t.hasIncompleteVersionstamp()) {
			return ByteArrayUtil.compareUnsigned(packed, t.packed);
		}
		else {
			// Must materialize to compare semantically for incomplete versionstamps
			return new IterableComparator().compare(materialize(), t.materialize());
		}
	}

	/**
	 * Returns a hash code value for this {@code Tuple}. Computing the hash code is fairly expensive
	 *  as it involves packing the underlying {@code Tuple} to bytes. However, this value is memoized,
	 *  so for any given {@code Tuple}, it only needs to be computed once. This means that it is
	 *  generally safe to use {@code Tuple}s with hash-based data structures such as
	 *  {@link java.util.HashSet HashSet}s or {@link java.util.HashMap HashMap}s.
	 * {@inheritDoc}
	 *
	 * @return a hash code for this {@code Tuple} that can be used by hash tables
	 */
	@Override
	public int hashCode() {
		if(memoizedHash == 0) {
			memoizedHash = Arrays.hashCode(packMaybeVersionstamp());
		}
		return memoizedHash;
	}

	/**
	 * Tests for equality with another {@code Tuple}. If the passed object is not a {@code Tuple}
	 *  this returns false. If the object is a {@code Tuple}, this returns true if
	 *  {@link Tuple#compareTo(Tuple) compareTo()} would return {@code 0}.
	 *
	 * @return {@code true} if {@code obj} is a {@code Tuple} and their binary representation
	 *  is identical
	 */
	@Override
	public boolean equals(Object o) {
		if(o == null)
			return false;
		if(o == this)
			return true;
		if(o instanceof Tuple) {
			return compareTo((Tuple)o) == 0;
		}
		return false;
	}

	/**
	 * Returns a string representing this {@code Tuple}. This contains human-readable
	 *  representations of all of the elements of this {@code Tuple}. For most elements,
	 *  this means using that object's default string representation. For byte-arrays,
	 *  this means using {@link ByteArrayUtil#printable(byte[]) ByteArrayUtil.printable()}
	 *  to produce a byte-string where most printable ASCII code points have been
	 *  rendered as characters.
	 *
	 * @return a human-readable {@link String} representation of this {@code Tuple}
	 */
	@Override
	public String toString() {
		List<Object> items = materialize();
		StringBuilder s = new StringBuilder("(");
		boolean first = true;

		for(Object o : items) {
			if(!first) {
				s.append(", ");
			}

			first = false;
			if(o == null) {
				s.append("null");
			}
			else if(o instanceof String) {
				s.append("\"");
				s.append(o);
				s.append("\"");
			}
			else if(o instanceof byte[]) {
				s.append("b\"");
				s.append(ByteArrayUtil.printable((byte[])o));
				s.append("\"");
			}
			else {
				s.append(o);
			}
		}

		s.append(")");
		return s.toString();
	}

	// -----------------------------------------------------------------------
	// Static factory methods
	// -----------------------------------------------------------------------

	/**
	 * Creates a new {@code Tuple} from a variable number of elements. The elements
	 *  must follow the type guidelines from {@link Tuple#addObject(Object) add}, and so
	 *  can only be {@link String}s, {@code byte[]}s, {@link Number}s, {@link UUID}s,
	 *  {@link Boolean}s, {@link List}s, {@code Tuple}s, or {@code null}s.
	 *
	 * @param items the elements from which to create the {@code Tuple}
	 *
	 * @return a new {@code Tuple} with the given items as its elements
	 */
	public static Tuple fromItems(Iterable<?> items) {
		if(items instanceof List<?>) {
			return Tuple.fromList((List<?>)items);
		}
		List<Object> elements = new ArrayList<>();
		for(Object o : items) {
			elements.add(o);
		}
		return fromList(elements);
	}

	/**
	 * Efficiently creates a new {@code Tuple} from a list of objects. The elements
	 *  must follow the type guidelines from {@link Tuple#addObject(Object) add}, and so
	 *  can only be {@link String}s, {@code byte[]}s, {@link Number}s, {@link UUID}s,
	 *  {@link Boolean}s, {@link List}s, {@code Tuple}s, or {@code null}s.
	 *
	 * @param items the elements from which to create the {@code Tuple}.
	 *
	 * @return a new {@code Tuple} with the given items as its elements
	 */
	public static Tuple fromList(List<?> items) {
		TupleCodec.validateStrings(items);
		byte[] p = packObjects(items);
		boolean hasIncomplete = TupleCodec.hasIncompleteVersionstamp(items);
		int count = items.size();
		return new Tuple(p, count, hasIncomplete);
	}

	/**
	 * Efficiently creates a new {@code Tuple} from a {@link Stream} of objects. The
	 *  elements must follow the type guidelines from {@link Tuple#addObject(Object) add},
	 *  and so can only be {@link String}s, {@code byte[]}s, {@link Number}s, {@link UUID}s,
	 *  {@link Boolean}s, {@link List}s, {@code Tuple}s, or {@code null}s. Note that this
	 *  class will consume all elements from the {@link Stream}.
	 *
	 * @param items the {@link Stream} of items from which to create the {@code Tuple}
	 *
	 * @return a new {@code Tuple} with the given items as its elements
	 */
	public static Tuple fromStream(Stream<?> items) {
		return fromList(items.collect(Collectors.toList()));
	}

	/**
	 * Creates a new {@code Tuple} from a variable number of elements. The elements
	 *  must follow the type guidelines from {@link Tuple#addObject(Object) add}, and so
	 *  can only be {@link String}s, {@code byte[]}s, {@link Number}s, {@link UUID}s,
	 *  {@link Boolean}s, {@link List}s, {@code Tuple}s, or {@code null}s.
	 *
	 * @param items the elements from which to create the {@code Tuple}
	 *
	 * @return a new {@code Tuple} with the given items as its elements
	 */
	public static Tuple from(Object... items) {
		return fromList(Arrays.asList(items));
	}

	static void main(String[] args) {
		for(int i : new int[] {10, 100, 1000, 10000, 100000, 1000000}) {
			createTuple(i);
		}

		Versionstamp completeVersionstamp = Versionstamp.complete(new byte[]{0x0f, (byte)0xdb, 0x0f, (byte)0xdb, 0x0f, (byte)0xdb, 0x0f, (byte)0x0db, 0x0f, (byte)0xdb}, 15);
		Versionstamp incompleteVersionstamp = Versionstamp.incomplete(20);

		Tuple t = new Tuple();
		t = t.add(Long.MAX_VALUE);
		t = t.add(Long.MAX_VALUE - 1);
		t = t.add(Long.MAX_VALUE - 2);
		t = t.add(1);
		t = t.add(0);
		t = t.add(-1);
		t = t.add(Long.MIN_VALUE + 2);
		t = t.add(Long.MIN_VALUE + 1);
		t = t.add(Long.MIN_VALUE);
		t = t.add("foo");
		t = t.addObject(null);
		t = t.add(false);
		t = t.add(true);
		t = t.add(3.14159);
		t = t.add(3.14159f);
		t = t.add(java.util.UUID.fromString("5fc03087-d265-11e7-b8c6-83e29cd24f4c"));
		t = t.add(t.getItems());
		t = t.add(t);
		t = t.add(new BigInteger("100000000000000000000000000000000000000000000"));
		t = t.add(new BigInteger("-100000000000000000000000000000000000000000000"));
		t = t.add(completeVersionstamp);
		byte[] bytes = t.pack();
		System.out.println("Packed: " + ByteArrayUtil.printable(bytes));
		List<Object> items = Tuple.fromBytes(bytes).getItems();
		for(Object obj : items) {
			if (obj != null)
				System.out.println(" -> type: (" + obj.getClass().getName() + "): " + obj);
			else
				System.out.println(" -> type: (null): null");
		}


		t = Tuple.fromStream(t.stream().map(item -> {
			if(item instanceof String) {
				return ((String)item).toUpperCase();
			} else {
				return item;
			}
		}));
		System.out.println("Upper cased: " + t);

		Tuple t2 = Tuple.fromBytes(bytes);
		System.out.println("t2.getLong(0): " + t2.getLong(0));
		System.out.println("t2.getBigInteger(1): " + t2.getBigInteger(1));
		System.out.println("t2.getString(9): " + t2.getString(9));
		System.out.println("t2.get(10): " + t2.get(10));
		System.out.println("t2.getBoolean(11): " + t2.getBoolean(11));
		System.out.println("t2.getBoolean(12): " + t2.getBoolean(12));
		System.out.println("t2.getDouble(13): " + t2.getDouble(13));
		System.out.println("t2.getFloat(13): " + t2.getFloat(13));
		System.out.println("t2.getLong(13): " + t2.getLong(13));
		System.out.println("t2.getBigInteger(13): " + t2.getBigInteger(13));
		System.out.println("t2.getDouble(14): " + t2.getDouble(14));
		System.out.println("t2.getFloat(14): " + t2.getFloat(14));
		System.out.println("t2.getLong(14): " + t2.getLong(14));
		System.out.println("t2.getBigInteger(14): " + t2.getBigInteger(14));
		System.out.println("t2.getNestedList(17): " + t2.getNestedList(17));
		System.out.println("t2.getNestedTuple(17): " + t2.getNestedTuple(17));
		System.out.println("t2.getVersionstamp(20): " + t2.getVersionstamp(20));

		int currOffset = 0;
		for (Object item : t) {
			int length = Tuple.from(item).pack().length;
			Tuple t3 = Tuple.fromBytes(bytes, currOffset, length);
			System.out.println("item = " + t3);
			currOffset += length;
		}

		System.out.println("(2*(Long.MAX_VALUE+1),) = " + ByteArrayUtil.printable(Tuple.from(
				BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE).shiftLeft(1)
		).pack()));
		System.out.println("(2*Long.MIN_VALUE,) = " + ByteArrayUtil.printable(Tuple.from(
				BigInteger.valueOf(Long.MIN_VALUE).multiply(new BigInteger("2"))
		).pack()));
		System.out.println("2*Long.MIN_VALUE = " + Tuple.fromBytes(Tuple.from(
				BigInteger.valueOf(Long.MIN_VALUE).multiply(new BigInteger("2"))).pack()).getBigInteger(0));

		Tuple vt1 = Tuple.from("complete:", completeVersionstamp, 1);
		System.out.println("vt1: " + vt1 + " ; has incomplete: " + vt1.hasIncompleteVersionstamp());
		Tuple vt2 = Tuple.from("incomplete:", incompleteVersionstamp, 2);
		System.out.println("vt2: " + vt2 + " ; has incomplete: " + vt2.hasIncompleteVersionstamp());
		Tuple vt3 = Tuple.from("complete nested: ", vt1, 3);
		System.out.println("vt3: " + vt3 + " ; has incomplete: " + vt3.hasIncompleteVersionstamp());
		Tuple vt4 = Tuple.from("incomplete nested: ", vt2, 4);
		System.out.println("vt4: " + vt4 + " ; has incomplete: " + vt4.hasIncompleteVersionstamp());
		Tuple vt5 = Tuple.from("complete with null: ", null, completeVersionstamp, 5);
		System.out.println("vt5: " + vt5 + " ; has incomplete: " + vt5.hasIncompleteVersionstamp());
		Tuple vt6 = Tuple.from("complete with null: ", null, incompleteVersionstamp, 6);
		System.out.println("vt6: " + vt6 + " ; has incomplete: " + vt6.hasIncompleteVersionstamp());
	}

	private static Tuple createTuple(int items) {
		List<Object> elements = new ArrayList<>(items);
		for(int i = 0; i < items; i++) {
			elements.add(new byte[]{99});
		}
		long start = System.currentTimeMillis();
		Tuple t = Tuple.fromList(elements);
		t.pack();
		System.out.println("Took " + (System.currentTimeMillis() - start) + " ms for " + items + " (" + elements.size() + ")");
		return t;
	}
}

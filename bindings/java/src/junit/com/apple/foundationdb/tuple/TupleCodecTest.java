/*
 * TupleCodecTest.java
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

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for TupleCodec low-level encode/decode, using hardcoded golden byte arrays
 * where possible to avoid self-referential validation through Tuple.from().pack().
 *
 * These tests do NOT require the FDB native library since TupleCodec does not
 * depend on FDB.instance().
 */
class TupleCodecTest {

	private static final byte FF = (byte)0xff;

	// -----------------------------------------------------------------------
	// Long encode/decode against golden bytes
	// -----------------------------------------------------------------------

	@Test
	void testEncodeLongZeroGolden() {
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeLong(buf, 0, 0L);
		Assertions.assertArrayEquals(new byte[] { 0x14 }, Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeLongOneGolden() {
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeLong(buf, 0, 1L);
		Assertions.assertArrayEquals(new byte[] { 0x15, 0x01 }, Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeLongNegOneGolden() {
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeLong(buf, 0, -1L);
		Assertions.assertArrayEquals(new byte[] { 0x13, FF - 1 }, Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeLong255Golden() {
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeLong(buf, 0, 255L);
		Assertions.assertArrayEquals(new byte[] { 0x15, FF }, Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeLongNeg255Golden() {
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeLong(buf, 0, -255L);
		Assertions.assertArrayEquals(new byte[] { 0x13, 0x00 }, Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeLong256Golden() {
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeLong(buf, 0, 256L);
		Assertions.assertArrayEquals(new byte[] { 0x16, 0x01, 0x00 }, Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeLongNeg256Golden() {
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeLong(buf, 0, -256L);
		Assertions.assertArrayEquals(new byte[] { 0x12, FF - 1, FF }, Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeLongMaxGolden() {
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeLong(buf, 0, Long.MAX_VALUE);
		Assertions.assertArrayEquals(new byte[] { 0x1C, 0x7f, FF, FF, FF, FF, FF, FF, FF }, Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeLongMinGolden() {
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeLong(buf, 0, Long.MIN_VALUE);
		Assertions.assertArrayEquals(new byte[] { 0x0C, 0x7f, FF, FF, FF, FF, FF, FF, FF }, Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeLongPositiveRange() {
		long[] values = {1, 127, 128, 255, 256, 65535, 65536, Integer.MAX_VALUE,
				(long)Integer.MAX_VALUE + 1, Long.MAX_VALUE - 1, Long.MAX_VALUE};
		for(long v : values) {
			byte[] expected = Tuple.from(v).pack();
			byte[] buf = new byte[16];
			int written = TupleCodec.encodeLong(buf, 0, v);
			Assertions.assertArrayEquals(expected, Arrays.copyOf(buf, written),
					"Mismatch for positive long " + v);
		}
	}

	@Test
	void testEncodeLongNegativeRange() {
		long[] values = {-1, -127, -128, -255, -256, -65535, -65536, Integer.MIN_VALUE,
				(long)Integer.MIN_VALUE - 1, Long.MIN_VALUE + 1, Long.MIN_VALUE};
		for(long v : values) {
			byte[] expected = Tuple.from(v).pack();
			byte[] buf = new byte[16];
			int written = TupleCodec.encodeLong(buf, 0, v);
			Assertions.assertArrayEquals(expected, Arrays.copyOf(buf, written),
					"Mismatch for negative long " + v);
		}
	}

	@Test
	void testDecodeLongRoundTrip() {
		long[] values = {0, 1, -1, 127, -128, 255, -256, 65535, -65536,
				Integer.MAX_VALUE, Integer.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE};
		for(long v : values) {
			byte[] buf = new byte[16];
			TupleCodec.encodeLong(buf, 0, v);
			long decoded = TupleCodec.decodeLong(buf, 0);
			Assertions.assertEquals(v, decoded, "Round-trip mismatch for long " + v);
		}
	}

	@Test
	void testEncodedLongSize() {
		Assertions.assertEquals(1, TupleCodec.encodedLongSize(0));
		Assertions.assertEquals(2, TupleCodec.encodedLongSize(1));
		Assertions.assertEquals(2, TupleCodec.encodedLongSize(-1));
		Assertions.assertEquals(2, TupleCodec.encodedLongSize(255));
		Assertions.assertEquals(3, TupleCodec.encodedLongSize(256));
		Assertions.assertEquals(9, TupleCodec.encodedLongSize(Long.MAX_VALUE));
		Assertions.assertEquals(9, TupleCodec.encodedLongSize(Long.MIN_VALUE));
	}

	// -----------------------------------------------------------------------
	// String encode/decode against golden bytes
	// -----------------------------------------------------------------------

	@Test
	void testEncodeStringEmptyGolden() {
		byte[] buf = new byte[64];
		int written = TupleCodec.encodeString(buf, 0, "");
		Assertions.assertArrayEquals(new byte[] { 0x02, 0x00 }, Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeStringHelloGolden() {
		byte[] buf = new byte[64];
		int written = TupleCodec.encodeString(buf, 0, "hello");
		Assertions.assertArrayEquals(new byte[] { 0x02, 'h', 'e', 'l', 'l', 'o', 0x00 }, Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeStringChineseGolden() {
		byte[] buf = new byte[64];
		int written = TupleCodec.encodeString(buf, 0, "\u4e2d\u6587");
		Assertions.assertArrayEquals(
				new byte[] { 0x02, (byte)0xe4, (byte)0xb8, (byte)0xad, (byte)0xe6, (byte)0x96, (byte)0x87, 0x00 },
				Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeStringFireEmojiGolden() {
		byte[] buf = new byte[64];
		int written = TupleCodec.encodeString(buf, 0, "\ud83d\udd25");
		Assertions.assertArrayEquals(
				new byte[] { 0x02, (byte)0xf0, (byte)0x9f, (byte)0x94, (byte)0xa5, 0x00 },
				Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeStringWithEmbeddedNull() {
		String s = "he\0llo";
		byte[] expected = Tuple.from(s).pack();
		byte[] buf = new byte[64];
		int written = TupleCodec.encodeString(buf, 0, s);
		Assertions.assertArrayEquals(expected, Arrays.copyOf(buf, written));
	}

	@Test
	void testDecodeStringRoundTrip() {
		String[] values = {"", "hello", "he\0llo", "\u021Aest", "A\uD83D\uDE00B", "\0\0\0"};
		for(String s : values) {
			byte[] buf = new byte[128];
			TupleCodec.encodeString(buf, 0, s);
			String decoded = TupleCodec.decodeString(buf, 0);
			Assertions.assertEquals(s, decoded, "Round-trip mismatch for string '" + s + "'");
		}
	}

	// -----------------------------------------------------------------------
	// Bytes encode/decode against golden bytes
	// -----------------------------------------------------------------------

	@Test
	void testEncodeBytesEmptyGolden() {
		byte[] buf = new byte[64];
		int written = TupleCodec.encodeBytes(buf, 0, new byte[0]);
		Assertions.assertArrayEquals(new byte[] { 0x01, 0x00 }, Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeBytesGolden() {
		byte[] buf = new byte[64];
		int written = TupleCodec.encodeBytes(buf, 0, new byte[] { 0x01, 0x02, 0x03 });
		Assertions.assertArrayEquals(new byte[] { 0x01, 0x01, 0x02, 0x03, 0x00 }, Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeBytesWithNullEscapingGolden() {
		byte[] buf = new byte[64];
		int written = TupleCodec.encodeBytes(buf, 0, new byte[] { 0x00, 0x00, 0x00, 0x04 });
		Assertions.assertArrayEquals(
				new byte[] { 0x01, 0x00, FF, 0x00, FF, 0x00, FF, 0x04, 0x00 },
				Arrays.copyOf(buf, written));
	}

	@Test
	void testDecodeBytesRoundTrip() {
		byte[][] values = {new byte[0], new byte[]{0x01, 0x02}, new byte[]{0x00}, new byte[]{0x00, 0x00, (byte)0xFF}};
		for(byte[] v : values) {
			byte[] buf = new byte[64];
			TupleCodec.encodeBytes(buf, 0, v);
			byte[] decoded = TupleCodec.decodeBytes(buf, 0);
			Assertions.assertArrayEquals(v, decoded, "Round-trip mismatch for bytes");
		}
	}

	// -----------------------------------------------------------------------
	// Float encode/decode against golden bytes
	// -----------------------------------------------------------------------

	@Test
	void testEncodeFloatPositiveGolden() {
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeFloat(buf, 0, 3.14f);
		Assertions.assertArrayEquals(
				new byte[] { 0x20, (byte)0xc0, 0x48, (byte)0xf5, (byte)0xc3 },
				Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeFloatNegativeGolden() {
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeFloat(buf, 0, -3.14f);
		Assertions.assertArrayEquals(
				new byte[] { 0x20, (byte)0x3f, (byte)0xb7, (byte)0x0a, (byte)0x3c },
				Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeFloatZeroGolden() {
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeFloat(buf, 0, 0.0f);
		Assertions.assertArrayEquals(new byte[] { 0x20, (byte)0x80, 0x00, 0x00, 0x00 }, Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeFloatNegZeroGolden() {
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeFloat(buf, 0, -0.0f);
		Assertions.assertArrayEquals(new byte[] { 0x20, 0x7f, FF, FF, FF }, Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeFloatInfinityGolden() {
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeFloat(buf, 0, Float.POSITIVE_INFINITY);
		Assertions.assertArrayEquals(new byte[] { 0x20, FF, (byte)0x80, 0x00, 0x00 }, Arrays.copyOf(buf, written));

		written = TupleCodec.encodeFloat(buf, 0, Float.NEGATIVE_INFINITY);
		Assertions.assertArrayEquals(new byte[] { 0x20, 0x00, 0x7f, FF, FF }, Arrays.copyOf(buf, written));
	}

	@Test
	void testDecodeFloatRoundTrip() {
		float[] values = {0.0f, -0.0f, 1.0f, -1.0f, Float.MAX_VALUE, Float.NaN,
				Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY};
		for(float v : values) {
			byte[] buf = new byte[16];
			TupleCodec.encodeFloat(buf, 0, v);
			float decoded = TupleCodec.decodeFloat(buf, 0);
			Assertions.assertEquals(Float.floatToRawIntBits(v), Float.floatToRawIntBits(decoded),
					"Round-trip mismatch for float " + v);
		}
	}

	// -----------------------------------------------------------------------
	// Double encode/decode against golden bytes
	// -----------------------------------------------------------------------

	@Test
	void testEncodeDoublePositiveGolden() {
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeDouble(buf, 0, 3.14);
		Assertions.assertArrayEquals(
				new byte[] { 0x21, (byte)0xc0, (byte)0x09, (byte)0x1e, (byte)0xb8,
				             (byte)0x51, (byte)0xeb, (byte)0x85, (byte)0x1f },
				Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeDoubleNegativeGolden() {
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeDouble(buf, 0, -3.14);
		Assertions.assertArrayEquals(
				new byte[] { 0x21, (byte)0x3f, (byte)0xf6, (byte)0xe1, (byte)0x47,
				             (byte)0xae, (byte)0x14, (byte)0x7a, (byte)0xe0 },
				Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeDoubleZeroGolden() {
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeDouble(buf, 0, 0.0);
		Assertions.assertArrayEquals(
				new byte[] { 0x21, (byte)0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 },
				Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeDoubleNegZeroGolden() {
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeDouble(buf, 0, -0.0);
		Assertions.assertArrayEquals(
				new byte[] { 0x21, 0x7f, FF, FF, FF, FF, FF, FF, FF },
				Arrays.copyOf(buf, written));
	}

	@Test
	void testDecodeDoubleRoundTrip() {
		double[] values = {0.0, -0.0, 1.0, -1.0, Double.MAX_VALUE, Double.NaN,
				Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
		for(double v : values) {
			byte[] buf = new byte[16];
			TupleCodec.encodeDouble(buf, 0, v);
			double decoded = TupleCodec.decodeDouble(buf, 0);
			Assertions.assertEquals(Double.doubleToRawLongBits(v), Double.doubleToRawLongBits(decoded),
					"Round-trip mismatch for double " + v);
		}
	}

	// -----------------------------------------------------------------------
	// Boolean encode/decode against golden bytes
	// -----------------------------------------------------------------------

	@Test
	void testEncodeBooleanGolden() {
		byte[] buf = new byte[2];
		int written = TupleCodec.encodeBoolean(buf, 0, true);
		Assertions.assertArrayEquals(new byte[] { 0x27 }, Arrays.copyOf(buf, written));

		written = TupleCodec.encodeBoolean(buf, 0, false);
		Assertions.assertArrayEquals(new byte[] { 0x26 }, Arrays.copyOf(buf, written));
	}

	@Test
	void testDecodeBooleanRoundTrip() {
		byte[] buf = new byte[1];
		TupleCodec.encodeBoolean(buf, 0, true);
		Assertions.assertTrue(TupleCodec.decodeBoolean(buf, 0));
		TupleCodec.encodeBoolean(buf, 0, false);
		Assertions.assertFalse(TupleCodec.decodeBoolean(buf, 0));
	}

	// -----------------------------------------------------------------------
	// Null encode against golden bytes
	// -----------------------------------------------------------------------

	@Test
	void testEncodeNullGolden() {
		byte[] buf = new byte[2];
		int written = TupleCodec.encodeNull(buf, 0);
		Assertions.assertArrayEquals(new byte[] { 0x00 }, Arrays.copyOf(buf, written));
	}

	// -----------------------------------------------------------------------
	// UUID encode/decode against golden bytes
	// -----------------------------------------------------------------------

	@Test
	void testEncodeUUIDGolden() {
		byte[] buf = new byte[32];
		int written = TupleCodec.encodeUUID(buf, 0, new UUID(0xba5eba11, 0x5ca1ab1e));
		Assertions.assertArrayEquals(
				new byte[] { 0x30, FF, FF, FF, FF, (byte)0xba, 0x5e, (byte)0xba, 0x11,
				             0x00, 0x00, 0x00, 0x00, 0x5c, (byte)0xa1, (byte)0xab, 0x1e },
				Arrays.copyOf(buf, written));
	}

	@Test
	void testDecodeUUIDRoundTrip() {
		UUID[] values = {new UUID(0, 0), new UUID(Long.MAX_VALUE, Long.MIN_VALUE),
		                 new UUID(0xba5eba11, 0x5ca1ab1e)};
		for(UUID v : values) {
			byte[] buf = new byte[32];
			TupleCodec.encodeUUID(buf, 0, v);
			UUID decoded = TupleCodec.decodeUUID(buf, 0);
			Assertions.assertEquals(v, decoded, "Round-trip mismatch for UUID " + v);
		}
	}

	// -----------------------------------------------------------------------
	// Versionstamp encode/decode
	// -----------------------------------------------------------------------

	@Test
	void testEncodeCompleteVersionstampGolden() {
		Versionstamp vs = Versionstamp.complete(
				new byte[] { (byte)0xaa, (byte)0xbb, (byte)0xcc, (byte)0xdd, (byte)0xee,
				             FF, 0x00, 0x01, 0x02, 0x03 });
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeVersionstamp(buf, 0, vs);
		Assertions.assertArrayEquals(
				new byte[] { 0x33, (byte)0xaa, (byte)0xbb, (byte)0xcc, (byte)0xdd, (byte)0xee,
				             FF, 0x00, 0x01, 0x02, 0x03, 0x00, 0x00 },
				Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeCompleteVersionstampWithUserVersionGolden() {
		Versionstamp vs = Versionstamp.complete(
				new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a }, 657);
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeVersionstamp(buf, 0, vs);
		Assertions.assertArrayEquals(
				new byte[] { 0x33, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
				             0x02, (byte)0x91 },
				Arrays.copyOf(buf, written));
	}

	@Test
	void testDecodeVersionstampRoundTrip() {
		Versionstamp vs = Versionstamp.complete(
				new byte[] { (byte)0xaa, (byte)0xbb, (byte)0xcc, (byte)0xdd, (byte)0xee,
				             FF, 0x00, 0x01, 0x02, 0x03 }, 42);
		byte[] buf = new byte[16];
		TupleCodec.encodeVersionstamp(buf, 0, vs);
		Versionstamp decoded = TupleCodec.decodeVersionstamp(buf, 0);
		Assertions.assertEquals(vs, decoded);
	}

	@Test
	void testEncodeIncompleteVersionstampRoundTrip() {
		Versionstamp vs = Versionstamp.incomplete(20);
		byte[] buf = new byte[16];
		int written = TupleCodec.encodeVersionstamp(buf, 0, vs);
		Assertions.assertEquals(13, written);
		Versionstamp decoded = TupleCodec.decodeVersionstamp(buf, 0);
		Assertions.assertEquals(vs, decoded);
		Assertions.assertFalse(decoded.isComplete());
	}

	// -----------------------------------------------------------------------
	// BigInteger encode/decode
	// -----------------------------------------------------------------------

	@Test
	void testEncodeBigIntegerLongMaxPlusOneGolden() {
		byte[] buf = new byte[300];
		int written = TupleCodec.encodeBigInteger(buf, 0, BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE));
		Assertions.assertArrayEquals(
				new byte[] { 0x1C, (byte)0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 },
				Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeBigInteger2Pow64Golden() {
		byte[] buf = new byte[300];
		int written = TupleCodec.encodeBigInteger(buf, 0, BigInteger.ONE.shiftLeft(64));
		Assertions.assertArrayEquals(
				new byte[] { 0x1D, 0x09, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 },
				Arrays.copyOf(buf, written));
	}

	@Test
	void testDecodeBigIntegerRoundTrip() {
		BigInteger[] values = {
			BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE),
			BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE),
			new BigInteger("100000000000000000000000000000000000000000000"),
			new BigInteger("-100000000000000000000000000000000000000000000"),
			BigInteger.ONE.shiftLeft(64),
			BigInteger.ONE.shiftLeft(64).negate(),
		};
		for(BigInteger v : values) {
			byte[] buf = new byte[300];
			TupleCodec.encodeBigInteger(buf, 0, v);
			BigInteger decoded = TupleCodec.decodeBigInteger(buf, 0);
			Assertions.assertEquals(v, decoded, "Round-trip mismatch for BigInteger " + v);
		}
	}

	// -----------------------------------------------------------------------
	// Nested tuple encode against golden bytes
	// -----------------------------------------------------------------------

	@Test
	void testEncodeNestedTupleNullAndStringGolden() {
		// Tuple.from(null, "hello") nested -> 0x05, 0x00, 0xFF, 0x02, 'h', 'e', 'l', 'l', 'o', 0x00, 0x00
		byte[] buf = new byte[128];
		int written = TupleCodec.encodeNested(buf, 0, Tuple.from(null, "hello"));
		Assertions.assertArrayEquals(
				new byte[] { 0x05, 0x00, FF, 0x02, 'h', 'e', 'l', 'l', 'o', 0x00, 0x00 },
				Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeNestedTupleNullOnlyGolden() {
		byte[] buf = new byte[128];
		int written = TupleCodec.encodeNested(buf, 0, Tuple.from((Object)null));
		Assertions.assertArrayEquals(new byte[] { 0x05, 0x00, FF, 0x00 }, Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeNestedListNullAndStringGolden() {
		byte[] buf = new byte[128];
		int written = TupleCodec.encodeNested(buf, 0, Arrays.asList(null, "hello"));
		Assertions.assertArrayEquals(
				new byte[] { 0x05, 0x00, FF, 0x02, 'h', 'e', 'l', 'l', 'o', 0x00, 0x00 },
				Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeNestedListWithEmbeddedNullStringGolden() {
		byte[] buf = new byte[128];
		int written = TupleCodec.encodeNested(buf, 0, Arrays.asList(null, "hell\0"));
		Assertions.assertArrayEquals(
				new byte[] { 0x05, 0x00, FF, 0x02, 'h', 'e', 'l', 'l', 0x00, FF, 0x00, 0x00 },
				Arrays.copyOf(buf, written));
	}

	@Test
	void testEncodeNestedEmptyList() {
		List<Object> nested = Collections.emptyList();
		byte[] expected = Tuple.from((Object)nested).pack();
		byte[] buf = new byte[128];
		int written = TupleCodec.encodeNested(buf, 0, nested);
		Assertions.assertArrayEquals(expected, Arrays.copyOf(buf, written));
	}

	// -----------------------------------------------------------------------
	// Element size for all types
	// -----------------------------------------------------------------------

	@Test
	void testElementSizeAllTypes() {
		Object[] values = {null, 0L, 42L, -42L, Long.MAX_VALUE, Long.MIN_VALUE,
				"hello", "he\0llo", "", new byte[0], new byte[]{0x00, 0x01},
				3.14f, -0.0f, 3.14, -0.0, true, false,
				new UUID(1, 2), BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE),
				Versionstamp.complete(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a })};
		for(Object v : values) {
			byte[] packed = Tuple.from(v).pack();
			int size = TupleCodec.elementSize(packed, 0);
			Assertions.assertEquals(packed.length, size,
					"elementSize mismatch for " + (v == null ? "null" : v.getClass().getSimpleName() + ":" + v));
		}
	}

	@Test
	void testElementSizeNestedTuple() {
		byte[] packed = Tuple.from(Tuple.from(null, "hello")).pack();
		int size = TupleCodec.elementSize(packed, 0);
		Assertions.assertEquals(packed.length, size, "elementSize mismatch for nested tuple");
	}

	@Test
	void testElementSizeNestedList() {
		byte[] packed = Tuple.from((Object)Arrays.asList(42L, "test")).pack();
		int size = TupleCodec.elementSize(packed, 0);
		Assertions.assertEquals(packed.length, size, "elementSize mismatch for nested list");
	}

	@Test
	void testElementSizeWithLimitDetectsTruncation() {
		// A valid encoded long that needs 9 bytes: INT_ZERO_CODE + 8 data bytes
		byte[] packed = Tuple.from(Long.MAX_VALUE).pack();
		// Provide limit that cuts off the last byte -> should throw
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> TupleCodec.elementSize(packed, 0, packed.length - 1));
	}

	// -----------------------------------------------------------------------
	// Counting and scanning
	// -----------------------------------------------------------------------

	@Test
	void testCountElements() {
		byte[] packed = Tuple.from("hello", 42L, null, true, 3.14).pack();
		Assertions.assertEquals(5, TupleCodec.countElements(packed, 0, packed.length));
	}

	@Test
	void testCountElementsEmpty() {
		Assertions.assertEquals(0, TupleCodec.countElements(new byte[0], 0, 0));
	}

	@Test
	void testStructuralCountAndScanNoVersionstamp() {
		byte[] packed = Tuple.from("hello", 42L, null).pack();
		TupleCodec.ScanResult result = TupleCodec.structuralCountAndScan(packed, 0, packed.length);
		Assertions.assertEquals(3, result.offsets.length, "element count");
		Assertions.assertFalse(result.hasIncompleteVersionstamp, "hasIncomplete should be false");
	}

	@Test
	void testStructuralCountAndScanWithIncompleteVersionstamp() {
		// Build packed bytes manually since pack() rejects incomplete versionstamps
		byte[] buf = new byte[64];
		int written = TupleCodec.encodeString(buf, 0, "hello");
		written += TupleCodec.encodeVersionstamp(buf, written, Versionstamp.incomplete(1));
		byte[] packed = Arrays.copyOf(buf, written);
		TupleCodec.ScanResult result = TupleCodec.structuralCountAndScan(packed, 0, packed.length);
		Assertions.assertEquals(2, result.offsets.length, "element count");
		Assertions.assertTrue(result.hasIncompleteVersionstamp, "hasIncomplete should be true");
	}

	@Test
	void testStructuralCountAndScanWithCompleteVersionstamp() {
		Versionstamp complete = Versionstamp.complete(
				new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a });
		byte[] packed = Tuple.from("hello", complete).pack();
		TupleCodec.ScanResult result = TupleCodec.structuralCountAndScan(packed, 0, packed.length);
		Assertions.assertEquals(2, result.offsets.length, "element count");
		Assertions.assertFalse(result.hasIncompleteVersionstamp, "hasIncomplete should be false for complete versionstamp");
	}

	@Test
	void testStructuralCountAndScanEmpty() {
		TupleCodec.ScanResult result = TupleCodec.structuralCountAndScan(new byte[0], 0, 0);
		Assertions.assertEquals(0, result.offsets.length);
		Assertions.assertFalse(result.hasIncompleteVersionstamp);
	}

	// -----------------------------------------------------------------------
	// scanForIncompleteVersionstamp / countIncompleteVersionstamps
	// -----------------------------------------------------------------------

	@Test
	void testScanIncompleteVersionstampInfoOffset() {
		// Encode "hello" then an incomplete versionstamp
		byte[] buf = new byte[64];
		int written = TupleCodec.encodeString(buf, 0, "hello");
		int helloSize = written;
		written += TupleCodec.encodeVersionstamp(buf, written, Versionstamp.incomplete(1));
		byte[] packed = Arrays.copyOf(buf, written);
		int[] info = TupleCodec.scanIncompleteVersionstampInfo(packed, 0, packed.length);
		Assertions.assertEquals(1, info[0], "count");
		Assertions.assertTrue(info[1] >= 0, "offset should be >= 0");
		// Offset should be past the "hello" encoding, then at the versionstamp data (pos+1)
		Assertions.assertEquals(helloSize + 1, info[1], "offset of first incomplete versionstamp data");
	}

	@Test
	void testScanIncompleteVersionstampInfoNone() {
		byte[] packed = Tuple.from("hello", 42L).pack();
		int[] info = TupleCodec.scanIncompleteVersionstampInfo(packed, 0, packed.length);
		Assertions.assertEquals(0, info[0], "count");
		Assertions.assertEquals(-1, info[1], "offset should be -1 when none found");
	}

	// -----------------------------------------------------------------------
	// compareItems
	// -----------------------------------------------------------------------

	@Test
	void testCompareItemsIntegers() {
		Assertions.assertTrue(TupleCodec.compareItems(0L, 1L) < 0);
		Assertions.assertTrue(TupleCodec.compareItems(1L, 0L) > 0);
		Assertions.assertEquals(0, TupleCodec.compareItems(42L, 42L));
		Assertions.assertTrue(TupleCodec.compareItems(-1L, 0L) < 0);
		Assertions.assertTrue(TupleCodec.compareItems(Long.MIN_VALUE, Long.MAX_VALUE) < 0);
	}

	@Test
	void testCompareItemsLongVsBigInteger() {
		// Long.MAX_VALUE < Long.MAX_VALUE + 1 (BigInteger)
		Assertions.assertTrue(TupleCodec.compareItems(
				Long.MAX_VALUE,
				BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)) < 0);
		// Long.MIN_VALUE > Long.MIN_VALUE - 1 (BigInteger)
		Assertions.assertTrue(TupleCodec.compareItems(
				Long.MIN_VALUE,
				BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE)) > 0);
	}

	@Test
	void testCompareItemsStrings() {
		Assertions.assertTrue(TupleCodec.compareItems("a", "b") < 0);
		Assertions.assertTrue(TupleCodec.compareItems("b", "a") > 0);
		Assertions.assertEquals(0, TupleCodec.compareItems("hello", "hello"));
		Assertions.assertTrue(TupleCodec.compareItems("", "a") < 0);
	}

	@Test
	void testCompareItemsCrossTypeOrdering() {
		// null < bytes < string < nested < int zero < ... (by type code)
		Assertions.assertTrue(TupleCodec.compareItems(null, new byte[0]) < 0);
		Assertions.assertTrue(TupleCodec.compareItems(new byte[0], "") < 0);
		Assertions.assertTrue(TupleCodec.compareItems("", 0L) < 0);
	}

	@Test
	void testCompareItemsFloatSpecialValues() {
		// -Infinity < -0.0 < 0.0 < Infinity < NaN (by IEEE float encoding after XOR)
		Assertions.assertTrue(TupleCodec.compareItems(Float.NEGATIVE_INFINITY, -0.0f) < 0);
		Assertions.assertTrue(TupleCodec.compareItems(-0.0f, 0.0f) < 0);
		Assertions.assertTrue(TupleCodec.compareItems(0.0f, Float.POSITIVE_INFINITY) < 0);
		Assertions.assertTrue(TupleCodec.compareItems(Float.POSITIVE_INFINITY, Float.NaN) < 0);
	}

	@Test
	void testCompareItemsDoubleSpecialValues() {
		Assertions.assertTrue(TupleCodec.compareItems(Double.NEGATIVE_INFINITY, -0.0) < 0);
		Assertions.assertTrue(TupleCodec.compareItems(-0.0, 0.0) < 0);
		Assertions.assertTrue(TupleCodec.compareItems(0.0, Double.POSITIVE_INFINITY) < 0);
		Assertions.assertTrue(TupleCodec.compareItems(Double.POSITIVE_INFINITY, Double.NaN) < 0);
	}

	@Test
	void testCompareItemsBooleans() {
		Assertions.assertTrue(TupleCodec.compareItems(false, true) < 0);
		Assertions.assertTrue(TupleCodec.compareItems(true, false) > 0);
		Assertions.assertEquals(0, TupleCodec.compareItems(true, true));
	}

	@Test
	void testCompareItemsBytes() {
		Assertions.assertTrue(TupleCodec.compareItems(new byte[]{0x01}, new byte[]{0x02}) < 0);
		Assertions.assertEquals(0, TupleCodec.compareItems(new byte[]{0x01}, new byte[]{0x01}));
		Assertions.assertTrue(TupleCodec.compareItems(new byte[0], new byte[]{0x00}) < 0);
	}

	// -----------------------------------------------------------------------
	// validateStrings
	// -----------------------------------------------------------------------

	@Test
	void testValidateStringsValidStrings() {
		Assertions.assertDoesNotThrow(() -> TupleCodec.validateStrings(Arrays.asList("hello", "world", 42L, null)));
	}

	@Test
	void testValidateStringsLoneSurrogate() {
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> TupleCodec.validateStrings(Arrays.asList("hello", "\ud83d")));
	}

	@Test
	void testValidateStringsInNestedList() {
		List<Object> nested = Arrays.asList("\ud83d", "ok");
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> TupleCodec.validateStrings(Arrays.asList("hello", nested)));
	}

	@Test
	void testValidateStringsEmptyList() {
		Assertions.assertDoesNotThrow(() -> TupleCodec.validateStrings(Collections.emptyList()));
	}

	// -----------------------------------------------------------------------
	// encodeObject / decodeObject dispatcher
	// -----------------------------------------------------------------------

	@Test
	void testEncodeObjectMatchesTuplePack() {
		Object[] items = {null, 0L, 42L, -1L, Long.MAX_VALUE, Long.MIN_VALUE,
				"hello", "he\0llo", "",
				new byte[]{0x01, 0x02}, new byte[0],
				3.14f, -0.0f, Float.NaN,
				3.14, -0.0, Double.NaN,
				true, false,
				new UUID(123, 456),
				BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE),
				BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE)};
		for(Object item : items) {
			byte[] expected = Tuple.from(item).pack();
			byte[] buf = new byte[300];
			int written = TupleCodec.encodeObject(buf, 0, item);
			Assertions.assertArrayEquals(expected, Arrays.copyOf(buf, written),
					"encodeObject mismatch for " + (item == null ? "null" : item.getClass().getSimpleName() + ":" + item));
		}
	}

	@Test
	void testDecodeObjectMatchesTupleGet() {
		Object[] items = {42L, "hello", null, true, 3.14f, 3.14, new UUID(123, 456)};
		byte[] packed = Tuple.from(items).pack();

		int pos = 0;
		for(int i = 0; i < items.length; i++) {
			Object decoded = TupleCodec.decodeObject(packed, pos);
			if(items[i] == null) {
				Assertions.assertNull(decoded, "Expected null at index " + i);
			} else if(items[i] instanceof Float) {
				Assertions.assertEquals(Float.floatToRawIntBits((Float)items[i]),
						Float.floatToRawIntBits((Float)decoded),
						"Float mismatch at index " + i);
			} else if(items[i] instanceof Double) {
				Assertions.assertEquals(Double.doubleToRawLongBits((Double)items[i]),
						Double.doubleToRawLongBits((Double)decoded),
						"Double mismatch at index " + i);
			} else {
				Assertions.assertEquals(items[i], decoded, "Mismatch at index " + i);
			}
			pos += TupleCodec.elementSize(packed, pos);
		}
	}

	// -----------------------------------------------------------------------
	// getCodeFor
	// -----------------------------------------------------------------------

	@Test
	void testGetCodeFor() {
		Assertions.assertEquals(0x00, TupleCodec.getCodeFor(null));
		Assertions.assertEquals(0x01, TupleCodec.getCodeFor(new byte[0]));
		Assertions.assertEquals(0x02, TupleCodec.getCodeFor("hello"));
		Assertions.assertEquals(0x14, TupleCodec.getCodeFor(0L));
		Assertions.assertEquals(0x14, TupleCodec.getCodeFor(BigInteger.ZERO));
		Assertions.assertEquals(0x20, TupleCodec.getCodeFor(0.0f));
		Assertions.assertEquals(0x21, TupleCodec.getCodeFor(0.0));
		Assertions.assertEquals(0x26, TupleCodec.getCodeFor(false));
		Assertions.assertEquals(0x30, TupleCodec.getCodeFor(new UUID(0, 0)));
		Assertions.assertEquals(0x33, TupleCodec.getCodeFor(Versionstamp.incomplete(1)));
	}

	// -----------------------------------------------------------------------
	// Edge cases
	// -----------------------------------------------------------------------

	@Test
	void testElementSizeUnknownTypeCodeThrows() {
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> TupleCodec.elementSize(new byte[] { FF }, 0));
	}

	@Test
	void testStructuralCountAndScanRejectsUnknownTypeCode() {
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> TupleCodec.structuralCountAndScan(new byte[] { FF }, 0, 1));
	}

	@Test
	void testDecodeBigIntegerBoundaryValues() {
		// 2^64 - 1 (max unsigned 64-bit)
		BigInteger maxUnsigned64 = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
		byte[] buf = new byte[300];
		TupleCodec.encodeBigInteger(buf, 0, maxUnsigned64);
		BigInteger decoded = TupleCodec.decodeBigInteger(buf, 0);
		Assertions.assertEquals(maxUnsigned64, decoded);

		// -(2^64 - 1)
		BigInteger negMaxUnsigned64 = maxUnsigned64.negate();
		TupleCodec.encodeBigInteger(buf, 0, negMaxUnsigned64);
		decoded = TupleCodec.decodeBigInteger(buf, 0);
		Assertions.assertEquals(negMaxUnsigned64, decoded);
	}

}

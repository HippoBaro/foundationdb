/*
 * TupleCodec.java
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
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * Low-level encoding and decoding utilities for the FoundationDB tuple layer binary format.
 *
 * <p>Operates directly on byte arrays to avoid object allocation, ByteBuffer overhead,
 * and intermediate copies. All encoding/decoding logic faithfully reproduces the behavior
 * of the old {@code TupleUtil}.
 */
final class TupleCodec {

	static final int NULL_CODE         = 0x00;
	static final int BYTES_CODE        = 0x01;
	static final int STRING_CODE       = 0x02;
	static final int NESTED_CODE       = 0x05;
	static final int NEG_INT_START     = 0x0b;
	static final int INT_ZERO_CODE     = 0x14;
	static final int POS_INT_END       = 0x1d;
	static final int FLOAT_CODE        = 0x20;
	static final int DOUBLE_CODE       = 0x21;
	static final int FALSE_CODE        = 0x26;
	static final int TRUE_CODE         = 0x27;
	static final int UUID_CODE         = 0x30;
	static final int VERSIONSTAMP_CODE = 0x33;

	private static final int UUID_BYTES = 2 * Long.BYTES;
	private static final BigInteger BIG_LONG_MIN_VALUE = BigInteger.valueOf(Long.MIN_VALUE);
	private static final BigInteger BIG_LONG_MAX_VALUE = BigInteger.valueOf(Long.MAX_VALUE);

	static boolean useOldVersionOffsetFormat() {
		return com.apple.foundationdb.FDB.instance().getAPIVersion() < 520;
	}

	private TupleCodec() {}

	// -----------------------------------------------------------------------
	// Encoding
	// -----------------------------------------------------------------------

	/**
	 * Encodes a null element. Always 1 byte.
	 * @return number of bytes written (always 1)
	 */
	static int encodeNull(byte[] buf, int pos) {
		buf[pos] = (byte)NULL_CODE;
		return 1;
	}

	/**
	 * Encodes a long value into the buffer at the given position.
	 * Faithfully reproduces {@code TupleUtil#encode(TupleUtil.EncodeState, long)}.
	 * @return the number of bytes written
	 */
	static int encodeLong(byte[] buf, int pos, long value) {
		if(value == 0L) {
			buf[pos] = (byte)INT_ZERO_CODE;
			return 1;
		}
		int n = minimalByteCount(value);
		buf[pos] = (byte)(INT_ZERO_CODE + (value >= 0 ? n : -n));
		// For positive: big-endian bytes of value.
		// For negative: big-endian bytes of (value - 1), i.e. one's complement.
		long val = Long.reverseBytes((value >= 0) ? value : (value - 1)) >> (Long.SIZE - 8 * n);
		for(int x = 0; x < n; x++) {
			buf[pos + 1 + x] = (byte)(val & 0xff);
			val >>= 8;
		}
		return 1 + n;
	}

	/**
	 * Encodes a UTF-8 string inline (character-by-character), with single-pass null escaping.
	 * Callers are responsible for validating the string via {@link StringUtil#validate(String)}
	 * before calling this method; lone surrogates will be encoded as 3-byte CESU-8 sequences.
	 * @return the number of bytes written
	 */
	static int encodeString(byte[] buf, int pos, String value) {
		buf[pos] = (byte)STRING_CODE;
		int written = 1;
		int len = value.length();
		for(int i = 0; i < len; i++) {
			char c = value.charAt(i);
			if(c < 0x80) {
				buf[pos + written++] = (byte)c;
				if(c == 0x00) {
					buf[pos + written++] = (byte)0xFF; // null escape
				}
			} else if(c < 0x800) {
				buf[pos + written++] = (byte)(0xC0 | (c >> 6));
				buf[pos + written++] = (byte)(0x80 | (c & 0x3F));
			} else if(Character.isHighSurrogate(c) && i + 1 < len && Character.isLowSurrogate(value.charAt(i + 1))) {
				int cp = Character.toCodePoint(c, value.charAt(++i));
				buf[pos + written++] = (byte)(0xF0 | (cp >> 18));
				buf[pos + written++] = (byte)(0x80 | ((cp >> 12) & 0x3F));
				buf[pos + written++] = (byte)(0x80 | ((cp >> 6) & 0x3F));
				buf[pos + written++] = (byte)(0x80 | (cp & 0x3F));
			} else {
				buf[pos + written++] = (byte)(0xE0 | (c >> 12));
				buf[pos + written++] = (byte)(0x80 | ((c >> 6) & 0x3F));
				buf[pos + written++] = (byte)(0x80 | (c & 0x3F));
			}
		}
		buf[pos + written++] = 0x00; // terminator
		return written;
	}

	/**
	 * Encodes raw bytes with single-pass null escaping.
	 * @return the number of bytes written
	 */
	static int encodeBytes(byte[] buf, int pos, byte[] value) {
		buf[pos] = (byte)BYTES_CODE;
		int written = 1;
		for(byte b : value) {
			buf[pos + written++] = b;
			if(b == 0x00) {
				buf[pos + written++] = (byte)0xFF; // null escape
			}
		}
		buf[pos + written++] = 0x00; // terminator
		return written;
	}

	/**
	 * Encodes a float value.
	 * @return the number of bytes written (always 5)
	 */
	static int encodeFloat(byte[] buf, int pos, float value) {
		buf[pos] = (byte)FLOAT_CODE;
		putIntBE(buf, pos + 1, encodeFloatBits(value));
		return 5;
	}

	/**
	 * Encodes a double value.
	 * @return the number of bytes written (always 9)
	 */
	static int encodeDouble(byte[] buf, int pos, double value) {
		buf[pos] = (byte)DOUBLE_CODE;
		putLongBE(buf, pos + 1, encodeDoubleBits(value));
		return 9;
	}

	/**
	 * Encodes a boolean value.
	 * @return the number of bytes written (always 1)
	 */
	static int encodeBoolean(byte[] buf, int pos, boolean value) {
		buf[pos] = value ? (byte)TRUE_CODE : (byte)FALSE_CODE;
		return 1;
	}

	/**
	 * Encodes a UUID value.
	 * @return the number of bytes written (always 17)
	 */
	static int encodeUUID(byte[] buf, int pos, UUID uuid) {
		buf[pos] = (byte)UUID_CODE;
		putLongBE(buf, pos + 1, uuid.getMostSignificantBits());
		putLongBE(buf, pos + 9, uuid.getLeastSignificantBits());
		return 17;
	}

	/**
	 * Encodes a BigInteger value.
	 * Faithfully reproduces {@code TupleUtil#encode(TupleUtil.EncodeState, BigInteger)}.
	 * @return the number of bytes written
	 */
	static int encodeBigInteger(byte[] buf, int pos, BigInteger value) {
		if(value.equals(BigInteger.ZERO)) {
			buf[pos] = (byte)INT_ZERO_CODE;
			return 1;
		}
		int n = minimalByteCount(value);
		if(n > 0xff) {
			throw new IllegalArgumentException("BigInteger magnitude is too large (more than 255 bytes)");
		}
		if(value.compareTo(BigInteger.ZERO) > 0) {
			byte[] bytes = value.toByteArray();
			if(n > Long.BYTES) {
				buf[pos] = (byte)POS_INT_END;
				buf[pos + 1] = (byte)n;
				System.arraycopy(bytes, bytes.length - n, buf, pos + 2, n);
				return 2 + n;
			} else {
				buf[pos] = (byte)(INT_ZERO_CODE + n);
				System.arraycopy(bytes, bytes.length - n, buf, pos + 1, n);
				return 1 + n;
			}
		} else {
			byte[] bytes = value.subtract(BigInteger.ONE).toByteArray();
			if(n > Long.BYTES) {
				buf[pos] = (byte)NEG_INT_START;
				buf[pos + 1] = (byte)(n ^ 0xff);
				if(bytes.length >= n) {
					System.arraycopy(bytes, bytes.length - n, buf, pos + 2, n);
				} else {
					// Pad with 0x00 bytes
					int pad = n - bytes.length;
					for(int x = 0; x < pad; x++) {
						buf[pos + 2 + x] = 0x00;
					}
					System.arraycopy(bytes, 0, buf, pos + 2 + pad, bytes.length);
				}
				return 2 + n;
			} else {
				buf[pos] = (byte)(INT_ZERO_CODE - n);
				if(bytes.length >= n) {
					System.arraycopy(bytes, bytes.length - n, buf, pos + 1, n);
				} else {
					int pad = n - bytes.length;
					for(int x = 0; x < pad; x++) {
						buf[pos + 1 + x] = 0x00;
					}
					System.arraycopy(bytes, 0, buf, pos + 1 + pad, bytes.length);
				}
				return 1 + n;
			}
		}
	}

	/**
	 * Encodes a Versionstamp value.
	 * @return the number of bytes written (always 1 + Versionstamp.LENGTH = 13)
	 */
	static int encodeVersionstamp(byte[] buf, int pos, Versionstamp v) {
		buf[pos] = (byte)VERSIONSTAMP_CODE;
		byte[] vBytes = v.getBytes();
		System.arraycopy(vBytes, 0, buf, pos + 1, vBytes.length);
		return 1 + Versionstamp.LENGTH;
	}

	/**
	 * Encodes a nested tuple (from a List).
	 * Faithfully reproduces {@code TupleUtil#encode(TupleUtil.EncodeState, List)}.
	 * @return the number of bytes written
	 */
	static int encodeNested(byte[] buf, int pos, List<?> items) {
		buf[pos] = (byte)NESTED_CODE;
		int written = 1;
		for(Object item : items) {
			if(item == null) {
				buf[pos + written++] = (byte)NULL_CODE;
				buf[pos + written++] = (byte)0xFF;
			} else {
				written += encodeObject(buf, pos + written, item);
			}
		}
		buf[pos + written++] = (byte)NULL_CODE; // terminator
		return written;
	}

	/**
	 * Encodes a nested Tuple.
	 * @return the number of bytes written
	 */
	static int encodeNested(byte[] buf, int pos, Tuple t) {
		buf[pos] = (byte)NESTED_CODE;
		int written = 1;
		byte[] tPacked = t.getPackedBytes();
		int tPos = 0;
		while(tPos < tPacked.length) {
			int code = tPacked[tPos] & 0xFF;
			if(code == NULL_CODE) {
				buf[pos + written++] = (byte)NULL_CODE;
				buf[pos + written++] = (byte)0xFF;
				tPos += 1;
			} else {
				int elemSize = elementSize(tPacked, tPos);
				System.arraycopy(tPacked, tPos, buf, pos + written, elemSize);
				written += elemSize;
				tPos += elemSize;
			}
		}
		buf[pos + written++] = (byte)NULL_CODE; // terminator
		return written;
	}

	/**
	 * Type-dispatching encode. Faithfully reproduces the type precedence from
	 * {@code TupleUtil#encode(TupleUtil.EncodeState, Object)}.
	 * @return the number of bytes written
	 */
	static int encodeObject(byte[] buf, int pos, Object o) {
		if(o == null)
			return encodeNull(buf, pos);
		if(o instanceof byte[])
			return encodeBytes(buf, pos, (byte[])o);
		if(o instanceof String)
			return encodeString(buf, pos, (String)o);
		if(o instanceof Float)
			return encodeFloat(buf, pos, (Float)o);
		if(o instanceof Double)
			return encodeDouble(buf, pos, (Double)o);
		if(o instanceof Boolean)
			return encodeBoolean(buf, pos, (Boolean)o);
		if(o instanceof UUID)
			return encodeUUID(buf, pos, (UUID)o);
		if(o instanceof BigInteger)
			return encodeBigInteger(buf, pos, (BigInteger)o);
		if(o instanceof Number)
			return encodeLong(buf, pos, ((Number)o).longValue());
		if(o instanceof Versionstamp)
			return encodeVersionstamp(buf, pos, (Versionstamp)o);
		if(o instanceof List<?>)
			return encodeNested(buf, pos, (List<?>)o);
		if(o instanceof Tuple)
			return encodeNested(buf, pos, (Tuple)o);
		throw new IllegalArgumentException("Unsupported data type: " + o.getClass().getName());
	}

	// -----------------------------------------------------------------------
	// Decoding
	// -----------------------------------------------------------------------

	/**
	 * Decodes a long value from tuple-encoded bytes.
	 * Faithfully reproduces the integer decoding path in {@code TupleUtil#decode}.
	 */
	static long decodeLong(byte[] data, int pos) {
		int code = data[pos] & 0xFF;
		if(code == INT_ZERO_CODE) {
			return 0L;
		}
		boolean positive = code > INT_ZERO_CODE;
		int n = positive ? code - INT_ZERO_CODE : INT_ZERO_CODE - code;
		int start = pos + 1;

		if(positive) {
			long res = 0L;
			for(int i = start; i < start + n; i++) {
				res = (res << 8) | (data[i] & 0xFFL);
			}
			return res;
		} else {
			long res = ~0L;
			for(int i = start; i < start + n; i++) {
				res = (res << 8) | (data[i] & 0xFFL);
			}
			return res + 1;
		}
	}

	/**
	 * Decodes a string value from tuple-encoded bytes.
	 * Fast path: if no escaped nulls, create String directly from source bytes.
	 * Slow path: unescape first.
	 * Rejects malformed UTF-8 to match official behavior.
	 */
	static String decodeString(byte[] data, int pos) {
		int start = pos + 1; // skip type code
		boolean[] hasEscapedNulls = {false};
		int end = findTerminator(data, start, data.length, hasEscapedNulls);
		if(end < 0) end = data.length;

		ByteBuffer byteBuffer;
		if(!hasEscapedNulls[0]) {
			byteBuffer = ByteBuffer.wrap(data, start, end - start);
		} else {
			byteBuffer = ByteBuffer.wrap(unescapeNulls(data, start, end));
		}

		try {
			CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
				.onMalformedInput(CodingErrorAction.REPORT)
				.onUnmappableCharacter(CodingErrorAction.REPORT);
			return decoder.decode(byteBuffer).toString();
		} catch(CharacterCodingException e) {
			throw new IllegalArgumentException("malformed UTF-8 string", e);
		}
	}

	/**
	 * Decodes raw bytes from tuple-encoded bytes.
	 * Fast path: if no escaped nulls, copy directly.
	 * Slow path: unescape first.
	 */
	static byte[] decodeBytes(byte[] data, int pos) {
		int start = pos + 1; // skip type code
		boolean[] hasEscapedNulls = {false};
		int end = findTerminator(data, start, data.length, hasEscapedNulls);
		if(end < 0) end = data.length;

		if(!hasEscapedNulls[0]) {
			return Arrays.copyOfRange(data, start, end);
		}
		return unescapeNulls(data, start, end);
	}

	/**
	 * Decodes a float from tuple-encoded bytes.
	 */
	static float decodeFloat(byte[] data, int pos) {
		return decodeFloatBits(getIntBE(data, pos + 1));
	}

	/**
	 * Decodes a double from tuple-encoded bytes.
	 */
	static double decodeDouble(byte[] data, int pos) {
		return decodeDoubleBits(getLongBE(data, pos + 1));
	}

	/**
	 * Decodes a boolean from tuple-encoded bytes.
	 */
	static boolean decodeBoolean(byte[] data, int pos) {
		return (data[pos] & 0xFF) == TRUE_CODE;
	}

	/**
	 * Decodes a UUID from tuple-encoded bytes.
	 */
	static UUID decodeUUID(byte[] data, int pos) {
		return new UUID(getLongBE(data, pos + 1), getLongBE(data, pos + 9));
	}

	/**
	 * Decodes a BigInteger from tuple-encoded bytes at a position where the type code
	 * is {@code POS_INT_END} or {@code NEG_INT_START} (i.e., values that don't fit in a long).
	 * Faithfully reproduces the BigInteger decoding path in {@code TupleUtil#decode}.
	 */
	static BigInteger decodeBigInteger(byte[] data, int pos) {
		int code = data[pos] & 0xFF;
		int start = pos + 1;

		if(code == POS_INT_END) {
			int n = data[start] & 0xFF;
			byte[] intBytes = new byte[n + 1];
			System.arraycopy(data, start + 1, intBytes, 1, n);
			return new BigInteger(intBytes);
		} else if(code == NEG_INT_START) {
			int n = (data[start] ^ 0xFF) & 0xFF;
			byte[] intBytes = new byte[n + 1];
			System.arraycopy(data, start + 1, intBytes, 1, n);
			BigInteger origValue = new BigInteger(intBytes);
			BigInteger offset = BigInteger.ONE.shiftLeft(n * 8).subtract(BigInteger.ONE);
			return origValue.subtract(offset);
		}

		// For codes in the regular integer range, check if it actually needs BigInteger
		boolean positive = code > INT_ZERO_CODE;
		int n = positive ? code - INT_ZERO_CODE : INT_ZERO_CODE - code;

		if(positive && (n < Long.BYTES || data[start] > 0)) {
			return BigInteger.valueOf(decodeLong(data, pos));
		} else if(!positive && (n < Long.BYTES || data[start] < 0)) {
			return BigInteger.valueOf(decodeLong(data, pos));
		} else {
			// 8-byte boundary case - might be BigInteger or might be Long
			byte[] longBytes = new byte[9];
			System.arraycopy(data, start, longBytes, longBytes.length - n, n);
			if(!positive) {
				for(int i = longBytes.length - n; i < longBytes.length; i++) {
					longBytes[i] = (byte)(longBytes[i] ^ 0xFF);
				}
			}
			BigInteger val = new BigInteger(longBytes);
			if(!positive) val = val.negate();
			return val;
		}
	}

	/**
	 * Decodes a Versionstamp from tuple-encoded bytes.
	 */
	static Versionstamp decodeVersionstamp(byte[] data, int pos) {
		int start = pos + 1;
		return Versionstamp.fromBytes(Arrays.copyOfRange(data, start, start + Versionstamp.LENGTH));
	}

	/**
	 * Type-dispatching decode that returns a boxed Object.
	 * Faithfully reproduces the type dispatch and return types of {@code TupleUtil#decode}.
	 * This is the only decode method that boxes -used at interface boundaries.
	 */
	static Object decodeObject(byte[] data, int pos) {
		int code = data[pos] & 0xFF;

		if(code == NULL_CODE) {
			return null;
		}
		if(code == BYTES_CODE) {
			return decodeBytes(data, pos);
		}
		if(code == STRING_CODE) {
			return decodeString(data, pos);
		}
		if(code == FLOAT_CODE) {
			return decodeFloat(data, pos);
		}
		if(code == DOUBLE_CODE) {
			return decodeDouble(data, pos);
		}
		if(code == FALSE_CODE) {
			return false;
		}
		if(code == TRUE_CODE) {
			return true;
		}
		if(code == UUID_CODE) {
			return decodeUUID(data, pos);
		}
		if(code == POS_INT_END) {
			return decodeBigInteger(data, pos);
		}
		if(code == NEG_INT_START) {
			return decodeBigInteger(data, pos);
		}
		if(code > NEG_INT_START && code < POS_INT_END) {
			// Regular integer - check if it fits in long or needs BigInteger
			if(code == INT_ZERO_CODE) {
				return 0L;
			}
			boolean positive = code > INT_ZERO_CODE;
			int n = positive ? code - INT_ZERO_CODE : INT_ZERO_CODE - code;
			int start = pos + 1;

			if(positive && (n < Long.BYTES || data[start] > 0)) {
				return decodeLong(data, pos);
			} else if(!positive && (n < Long.BYTES || data[start] < 0)) {
				return decodeLong(data, pos);
			} else {
				// 8-byte boundary case - delegate to decodeBigInteger and narrow to long if possible
				BigInteger val = decodeBigInteger(data, pos);
				if(val.compareTo(BIG_LONG_MIN_VALUE) >= 0 && val.compareTo(BIG_LONG_MAX_VALUE) <= 0) {
					return val.longValue();
				}
				return val;
			}
		}
		if(code == VERSIONSTAMP_CODE) {
			return decodeVersionstamp(data, pos);
		}
		if(code == NESTED_CODE) {
			return decodeNestedTuple(data, pos);
		}
		throw new IllegalArgumentException("Unknown tuple data type " + code + " at index " + pos);
	}

	/**
	 * Decodes a nested tuple as a {@link Tuple}.
	 * The nested encoding uses {@code 0x00 0xFF} for null elements (to distinguish
	 * from the {@code 0x00} terminator). This method extracts the inner bytes,
	 * un-escapes null elements back to {@code 0x00}, and constructs a Tuple from
	 * the resulting flat packed bytes.
	 */
	private static Tuple decodeNestedTuple(byte[] data, int pos) {
		int totalSize = elementSize(data, pos);
		int innerStart = pos + 1;        // skip NESTED_CODE
		int innerEnd = pos + totalSize - 1; // exclude terminator 0x00

		// Count null elements to determine un-escaped size.
		// Each null is 0x00 0xFF (2 bytes) in nested, but 0x00 (1 byte) in flat.
		int nullCount = 0;
		int i = innerStart;
		while(i < innerEnd) {
			if(data[i] == 0x00) {
				// Must be 0x00 0xFF (null-in-nested); raw 0x00 can't appear here
				nullCount++;
				i += 2;
			} else {
				i += elementSize(data, i);
			}
		}

		int flatSize = (innerEnd - innerStart) - nullCount; // remove one byte per null escape
		if(flatSize == 0) {
			return new Tuple();
		}

		byte[] flat = new byte[flatSize];
		int srcPos = innerStart;
		int dstPos = 0;
		while(srcPos < innerEnd) {
			if(data[srcPos] == 0x00) {
				// Null-in-nested: 0x00 0xFF -> 0x00
				flat[dstPos++] = 0x00;
				srcPos += 2;
			} else {
				int elemSize = elementSize(data, srcPos);
				System.arraycopy(data, srcPos, flat, dstPos, elemSize);
				dstPos += elemSize;
				srcPos += elemSize;
			}
		}

		return Tuple.fromBytes(flat);
	}

	// -----------------------------------------------------------------------
	// Size calculation
	// -----------------------------------------------------------------------

	/**
	 * Like {@link #elementSize(byte[], int, int)} but uses data.length as the limit.
	 */
	static int elementSize(byte[] data, int pos) {
		return elementSize(data, pos, data.length);
	}

	/**
	 * Returns the total number of bytes consumed by the element starting at {@code pos}
	 * (including the type code byte). {@code limit} is the exclusive upper bound of
	 * valid data; the method throws if the element extends beyond it.
	 */
	static int elementSize(byte[] data, int pos, int limit) {
		if(pos >= limit) {
			throw new IllegalArgumentException("Element position " + pos + " is at or beyond limit " + limit);
		}
		int code = data[pos] & 0xFF;

		if(code == NULL_CODE || code == FALSE_CODE || code == TRUE_CODE) {
			return 1;
		}

		if(code == STRING_CODE || code == BYTES_CODE) {
			int term = findTerminator(data, pos + 1, limit, new boolean[1]);
			if(term < 0) {
				throw new IllegalArgumentException("Unterminated string/bytes at position " + pos);
			}
			return (term + 1) - pos;
		}

		if(code == NESTED_CODE) {
			int i = pos + 1;
			while(i < limit) {
				if(data[i] == 0x00) {
					if(i + 1 < limit && (data[i + 1] & 0xFF) == 0xFF) {
						i += 2; // null element inside nested tuple
					} else {
						return (i + 1) - pos; // terminator
					}
				} else {
					i += elementSize(data, i, limit); // skip inner element recursively
				}
			}
			throw new IllegalArgumentException("Unterminated nested tuple at position " + pos);
		}

		if(code == INT_ZERO_CODE) {
			return 1;
		}

		if(code > INT_ZERO_CODE && code < POS_INT_END) {
			int size = 1 + (code - INT_ZERO_CODE);
			if(pos + size > limit) {
				throw new IllegalArgumentException("Truncated integer at position " + pos);
			}
			return size;
		}
		if(code > NEG_INT_START && code < INT_ZERO_CODE) {
			int size = 1 + (INT_ZERO_CODE - code);
			if(pos + size > limit) {
				throw new IllegalArgumentException("Truncated integer at position " + pos);
			}
			return size;
		}

		if(code == POS_INT_END) {
			if(pos + 1 >= limit) {
				throw new IllegalArgumentException("Truncated big integer at position " + pos);
			}
			int n = data[pos + 1] & 0xFF;
			int size = 2 + n;
			if(pos + size > limit) {
				throw new IllegalArgumentException("Truncated big integer at position " + pos);
			}
			return size;
		}
		if(code == NEG_INT_START) {
			if(pos + 1 >= limit) {
				throw new IllegalArgumentException("Truncated big integer at position " + pos);
			}
			int n = (data[pos + 1] ^ 0xFF) & 0xFF;
			int size = 2 + n;
			if(pos + size > limit) {
				throw new IllegalArgumentException("Truncated big integer at position " + pos);
			}
			return size;
		}

		if(code == FLOAT_CODE) {
			int size = 1 + Float.BYTES;
			if(pos + size > limit) {
				throw new IllegalArgumentException("Truncated float at position " + pos);
			}
			return size;
		}
		if(code == DOUBLE_CODE) {
			int size = 1 + Double.BYTES;
			if(pos + size > limit) {
				throw new IllegalArgumentException("Truncated double at position " + pos);
			}
			return size;
		}
		if(code == UUID_CODE) {
			int size = 1 + UUID_BYTES;
			if(pos + size > limit) {
				throw new IllegalArgumentException("Truncated UUID at position " + pos);
			}
			return size;
		}
		if(code == VERSIONSTAMP_CODE) {
			int size = 1 + Versionstamp.LENGTH;
			if(pos + size > limit) {
				throw new IllegalArgumentException("Truncated versionstamp at position " + pos);
			}
			return size;
		}

		throw new IllegalArgumentException("Unknown tuple data type " + code + " at index " + pos);
	}

	/**
	 * Returns the exact number of bytes needed to encode a long value.
	 */
	static int encodedLongSize(long value) {
		if(value == 0) return 1;
		return 1 + minimalByteCount(value);
	}

	/**
	 * Returns the exact number of bytes needed to encode a BigInteger value.
	 */
	static int encodedBigIntegerSize(BigInteger value) {
		if(value.equals(BigInteger.ZERO)) return 1;
		int n = minimalByteCount(value);
		return (n > Long.BYTES) ? 2 + n : 1 + n;
	}

	/**
	 * Returns the exact encoded size for an Object.
	 * Faithfully reproduces {@code TupleUtil#getPackedSize(List, boolean)} element-level logic.
	 */
	static int encodedObjectSize(Object item) {
		if(item == null) return 1;
		if(item instanceof byte[]) {
			byte[] bytes = (byte[])item;
			return 2 + bytes.length + ByteArrayUtil.nullCount(bytes);
		}
		if(item instanceof String) {
			return 2 + StringUtil.packedSize((String)item);
		}
		if(item instanceof Float) return 1 + Float.BYTES;
		if(item instanceof Double) return 1 + Double.BYTES;
		if(item instanceof Boolean) return 1;
		if(item instanceof UUID) return 1 + UUID_BYTES;
		if(item instanceof BigInteger) {
			return encodedBigIntegerSize((BigInteger)item);
		}
		if(item instanceof Number) {
			return encodedLongSize(((Number)item).longValue());
		}
		if(item instanceof Versionstamp) {
			return 1 + Versionstamp.LENGTH;
		}
		if(item instanceof List<?>) {
			return 2 + encodedNestedSize((List<?>)item);
		}
		if(item instanceof Tuple) {
			return 2 + encodedNestedTupleSize((Tuple)item);
		}
		throw new IllegalArgumentException("Unknown type " + item.getClass() + " for tuple packing");
	}

	/**
	 * Returns the encoded size of a nested list's contents (without the NESTED_CODE and terminator).
	 */
	private static int encodedNestedSize(List<?> items) {
		int size = 0;
		for(Object item : items) {
			if(item == null) {
				size += 2; // NULL_CODE + 0xFF escape in nested context
			} else {
				size += encodedObjectSize(item);
			}
		}
		return size;
	}

	/**
	 * Returns the encoded size of a nested Tuple's contents (without the NESTED_CODE and terminator).
	 */
	private static int encodedNestedTupleSize(Tuple t) {
		byte[] tPacked = t.getPackedBytes();
		// Count null elements which will need an extra escape byte in nested context
		int nullCount = 0;
		int pos = 0;
		while(pos < tPacked.length) {
			if((tPacked[pos] & 0xFF) == NULL_CODE) {
				nullCount++;
			}
			pos += elementSize(tPacked, pos);
		}
		return tPacked.length + nullCount;
	}

	// -----------------------------------------------------------------------
	// Scanning
	// -----------------------------------------------------------------------

	/**
	 * Counts the number of top-level elements in the packed bytes between offset and end.
	 * Lightweight: only reads type codes and skips via elementSize(), no decoding.
	 */
	static int countElements(byte[] data, int offset, int end) {
		int count = 0;
		int pos = offset;
		while(pos < end) {
			pos += elementSize(data, pos, end);
			count++;
		}
		return count;
	}

	/**
	 * Result of a structural scan: element offsets and incomplete versionstamp flag.
	 */
	static final class ScanResult {
		final int[] offsets;
		final boolean hasIncompleteVersionstamp;

		ScanResult(int[] offsets, boolean hasIncompleteVersionstamp) {
			this.offsets = offsets;
			this.hasIncompleteVersionstamp = hasIncompleteVersionstamp;
		}
	}

	/**
	 * Single-pass structural validation and scanning of packed bytes. Builds the element
	 * offset index, validates structural integrity via {@link #elementSize(byte[], int, int)},
	 * and detects incomplete versionstamps. Does NOT decode element content (e.g., does not
	 * validate UTF-8 in strings). Content validation is deferred to access time.
	 * @throws IllegalArgumentException if any element has structural issues (truncation, unknown type, etc.)
	 */
	static ScanResult structuralCountAndScan(byte[] data, int offset, int end) {
		// Single pass: build offsets and detect incomplete versionstamps.
		int capacity = Math.min(16, (end - offset));
		int[] offsets = new int[capacity];
		int count = 0;
		boolean hasIncomplete = false;
		int pos = offset;
		while(pos < end) {
			if(count == capacity) {
				capacity = capacity + (capacity >> 1) + 1;
				offsets = Arrays.copyOf(offsets, capacity);
			}
			offsets[count] = pos;
			int code = data[pos] & 0xFF;
			int size = elementSize(data, pos, end);
			if(!hasIncomplete) {
				if(code == VERSIONSTAMP_CODE) {
					if(Versionstamp.isIncompleteAt(data, pos + 1)) {
						hasIncomplete = true;
					}
				} else if(code == NESTED_CODE) {
					int[] vsResult = {0, -1};
					scanIncompleteVersionstamps(data, pos + 1, pos + size - 1, vsResult);
					if(vsResult[0] > 0) hasIncomplete = true;
				}
			}
			pos += size;
			count++;
		}
		if(count != capacity) {
			offsets = Arrays.copyOf(offsets, count);
		}
		return new ScanResult(offsets, hasIncomplete);
	}

	/**
	 * Scans packed bytes for incomplete Versionstamps (including nested tuples).
	 * Returns the count and offset of the first one in a single traversal.
	 * result[0] = count of incomplete versionstamps, result[1] = offset of first (-1 if none).
	 */
	private static void scanIncompleteVersionstamps(byte[] data, int offset, int end, int[] result) {
		// This method operates on nested tuple content, where null elements are encoded
		// as 0x00 0xFF (not bare 0x00 which is the nested terminator).
		int pos = offset;
		while(pos < end) {
			int code = data[pos] & 0xFF;
			if(code == NULL_CODE) {
				// Null-in-nested: 0x00 0xFF -- skip the escape pair.
				// (A bare 0x00 without 0xFF would be a nested terminator, which shouldn't
				// appear within the range we're given.)
				pos += 2;
			} else if(code == VERSIONSTAMP_CODE) {
				if(Versionstamp.isIncompleteAt(data, pos + 1)) {
					if(result[1] < 0) {
						result[1] = pos + 1;
					}
					result[0]++;
				}
				pos += elementSize(data, pos, end);
			} else if(code == NESTED_CODE) {
				// Recurse into nested-within-nested. Find the end of this inner nested
				// tuple by scanning for the terminating 0x00 (not followed by 0xFF).
				int innerEnd = pos + elementSize(data, pos, end);
				// Inner content is between (pos + 1) and (innerEnd - 1), exclusive of
				// the NESTED_CODE prefix and NULL_CODE terminator.
				scanIncompleteVersionstamps(data, pos + 1, innerEnd - 1, result);
				pos = innerEnd;
			} else {
				pos += elementSize(data, pos, end);
			}
		}
	}

	/**
	 * Top-level scan for incomplete Versionstamps in packed (non-nested) bytes.
	 * Uses elementSize() to step over elements correctly at the top level, and delegates
	 * to scanIncompleteVersionstamps() only for the content of NESTED_CODE elements.
	 * Returns [count, firstOffset], where firstOffset is -1 if none found.
	 */
	private static int[] scanIncompleteVersionstampsTopLevel(byte[] data, int offset, int end) {
		int count = 0;
		int firstOffset = -1;
		int pos = offset;
		while(pos < end) {
			int code = data[pos] & 0xFF;
			int size = elementSize(data, pos, end);
			if(code == VERSIONSTAMP_CODE) {
				if(Versionstamp.isIncompleteAt(data, pos + 1)) {
					if(firstOffset < 0) firstOffset = pos + 1;
					count++;
				}
			} else if(code == NESTED_CODE) {
				int[] inner = {0, -1};
				scanIncompleteVersionstamps(data, pos + 1, pos + size - 1, inner);
				if(inner[0] > 0) {
					if(firstOffset < 0) firstOffset = inner[1];
					count += inner[0];
				}
			}
			pos += size;
		}
		return new int[]{count, firstOffset};
	}

	/**
	 * Scans packed bytes for incomplete Versionstamps and returns [count, firstOffset].
	 * firstOffset is -1 if no incomplete Versionstamp is found.
	 */
	static int[] scanIncompleteVersionstampInfo(byte[] data, int offset, int end) {
		return scanIncompleteVersionstampsTopLevel(data, offset, end);
	}

	// -----------------------------------------------------------------------
	// Float/Double bit encoding
	// -----------------------------------------------------------------------

	static int encodeFloatBits(float f) {
		int intBits = Float.floatToRawIntBits(f);
		return (intBits < 0) ? (~intBits) : (intBits ^ Integer.MIN_VALUE);
	}

	static long encodeDoubleBits(double d) {
		long longBits = Double.doubleToRawLongBits(d);
		return (longBits < 0L) ? (~longBits) : (longBits ^ Long.MIN_VALUE);
	}

	private static float decodeFloatBits(int i) {
		int origBits = (i >= 0) ? (~i) : (i ^ Integer.MIN_VALUE);
		return Float.intBitsToFloat(origBits);
	}

	private static double decodeDoubleBits(long l) {
		long origBits = (l >= 0) ? (~l) : (l ^ Long.MIN_VALUE);
		return Double.longBitsToDouble(origBits);
	}

	// -----------------------------------------------------------------------
	// Big-endian byte-order helpers
	// -----------------------------------------------------------------------

	private static void putIntBE(byte[] buf, int pos, int value) {
		buf[pos]     = (byte)(value >> 24);
		buf[pos + 1] = (byte)(value >> 16);
		buf[pos + 2] = (byte)(value >> 8);
		buf[pos + 3] = (byte)(value);
	}

	private static int getIntBE(byte[] buf, int pos) {
		return ((buf[pos] & 0xFF) << 24) |
			   ((buf[pos + 1] & 0xFF) << 16) |
			   ((buf[pos + 2] & 0xFF) << 8) |
			   (buf[pos + 3] & 0xFF);
	}

	private static void putLongBE(byte[] buf, int pos, long value) {
		buf[pos]     = (byte)(value >> 56);
		buf[pos + 1] = (byte)(value >> 48);
		buf[pos + 2] = (byte)(value >> 40);
		buf[pos + 3] = (byte)(value >> 32);
		buf[pos + 4] = (byte)(value >> 24);
		buf[pos + 5] = (byte)(value >> 16);
		buf[pos + 6] = (byte)(value >> 8);
		buf[pos + 7] = (byte)(value);
	}

	private static long getLongBE(byte[] buf, int pos) {
		return ((long)(buf[pos] & 0xFF) << 56) |
			   ((long)(buf[pos + 1] & 0xFF) << 48) |
			   ((long)(buf[pos + 2] & 0xFF) << 40) |
			   ((long)(buf[pos + 3] & 0xFF) << 32) |
			   ((long)(buf[pos + 4] & 0xFF) << 24) |
			   ((long)(buf[pos + 5] & 0xFF) << 16) |
			   ((long)(buf[pos + 6] & 0xFF) << 8) |
			   ((long)(buf[pos + 7] & 0xFF));
	}

	// -----------------------------------------------------------------------
	// Null-terminated field helpers
	// -----------------------------------------------------------------------

	/**
	 * Scans for the null terminator ({@code 0x00} not followed by {@code 0xFF})
	 * in a null-escaped byte region starting at {@code start} up to {@code limit}.
	 * Returns the terminator position, or -1 if not found.
	 * Sets {@code hasEscapedNulls[0]} to {@code true} if escaped nulls ({@code 0x00 0xFF}) were encountered.
	 */
	private static int findTerminator(byte[] data, int start, int limit, boolean[] hasEscapedNulls) {
		int i = start;
		while(i < limit) {
			if(data[i] == 0x00) {
				if(i + 1 < limit && (data[i + 1] & 0xFF) == 0xFF) {
					hasEscapedNulls[0] = true;
					i += 2;
				} else {
					return i;
				}
			} else {
				i++;
			}
		}
		return -1;
	}

	/**
	 * Un-escapes null bytes in a region: replaces {@code 0x00 0xFF} pairs with {@code 0x00}.
	 * Returns a new byte array with the unescaped content.
	 */
	private static byte[] unescapeNulls(byte[] data, int start, int end) {
		byte[] unescaped = new byte[end - start];
		int len = 0;
		for(int i = start; i < end; ) {
			if(data[i] == 0x00 && i + 1 < end && (data[i + 1] & 0xFF) == 0xFF) {
				unescaped[len++] = 0x00;
				i += 2;
			} else {
				unescaped[len++] = data[i++];
			}
		}
		return Arrays.copyOf(unescaped, len);
	}

	// -----------------------------------------------------------------------
	// Size helpers
	// -----------------------------------------------------------------------

	private static int minimalByteCount(long i) {
		return (Long.SIZE + 7 - Long.numberOfLeadingZeros(i >= 0 ? i : -i)) / 8;
	}

	private static int minimalByteCount(BigInteger i) {
		int bitLength = (i.compareTo(BigInteger.ZERO) >= 0) ? i.bitLength() : i.negate().bitLength();
		return (bitLength + 7) / 8;
	}


	// -----------------------------------------------------------------------
	// Item comparison (used by IterableComparator for semantic ordering)
	// -----------------------------------------------------------------------

	private static final IterableComparator iterableComparator = new IterableComparator();

	static int getCodeFor(Object o) {
		if(o == null)
			return NULL_CODE;
		if(o instanceof byte[])
			return BYTES_CODE;
		if(o instanceof String)
			return STRING_CODE;
		if(o instanceof Float)
			return FLOAT_CODE;
		if(o instanceof Double)
			return DOUBLE_CODE;
		if(o instanceof Boolean)
			return FALSE_CODE;
		if(o instanceof UUID)
			return UUID_CODE;
		if(o instanceof Number)
			return INT_ZERO_CODE;
		if(o instanceof Versionstamp)
			return VERSIONSTAMP_CODE;
		if(o instanceof List<?>)
			return NESTED_CODE;
		if(o instanceof Tuple)
			return NESTED_CODE;
		throw new IllegalArgumentException("Unsupported data type: " + o.getClass().getName());
	}

	static int compareItems(Object item1, Object item2) {
		if(item1 == item2) {
			return 0;
		}
		int code1 = getCodeFor(item1);
		int code2 = getCodeFor(item2);

		if(code1 != code2) {
			return Integer.compare(code1, code2);
		}

		if(code1 == NULL_CODE) {
			return 0;
		}
		if(code1 == BYTES_CODE) {
			return ByteArrayUtil.compareUnsigned((byte[])item1, (byte[])item2);
		}
		if(code1 == STRING_CODE) {
			return StringUtil.compareUtf8((String)item1, (String)item2);
		}
		if(code1 == INT_ZERO_CODE) {
			if(item1 instanceof Long && item2 instanceof Long) {
				return Long.compare((Long)item1, (Long)item2);
			}
			else {
				BigInteger bi1;
				if (item1 instanceof BigInteger) {
					bi1 = (BigInteger) item1;
				} else {
					bi1 = BigInteger.valueOf(((Number) item1).longValue());
				}
				BigInteger bi2;
				if (item2 instanceof BigInteger) {
					bi2 = (BigInteger) item2;
				} else {
					bi2 = BigInteger.valueOf(((Number) item2).longValue());
				}
				return bi1.compareTo(bi2);
			}
		}
		if(code1 == FLOAT_CODE) {
			int fbits1 = encodeFloatBits((Float)item1);
			int fbits2 = encodeFloatBits((Float)item2);
			return Integer.compareUnsigned(fbits1, fbits2);
		}
		if(code1 == DOUBLE_CODE) {
			long dbits1 = encodeDoubleBits((Double)item1);
			long dbits2 = encodeDoubleBits((Double)item2);
			return Long.compareUnsigned(dbits1, dbits2);
		}
		if(code1 == FALSE_CODE) {
			return Boolean.compare((Boolean)item1, (Boolean)item2);
		}
		if(code1 == UUID_CODE) {
			UUID uuid1 = (UUID)item1;
			UUID uuid2 = (UUID)item2;
			int cmp1 = Long.compareUnsigned(uuid1.getMostSignificantBits(), uuid2.getMostSignificantBits());
			if(cmp1 != 0)
				return cmp1;
			return Long.compareUnsigned(uuid1.getLeastSignificantBits(), uuid2.getLeastSignificantBits());
		}
		if(code1 == VERSIONSTAMP_CODE) {
			return ((Versionstamp)item1).compareTo((Versionstamp)item2);
		}
		if(code1 == NESTED_CODE) {
			return iterableComparator.compare((Iterable<?>)item1, (Iterable<?>)item2);
		}
		throw new IllegalArgumentException("Unknown tuple data type: " + item1.getClass());
	}

	// -----------------------------------------------------------------------
	// Collection-level helpers
	// -----------------------------------------------------------------------

	/**
	 * Determines if any element in the given collection (possibly nested) contains
	 * an incomplete Versionstamp.
	 */
	static boolean hasIncompleteVersionstamp(Collection<?> items) {
		for (Object item: items) {
			if(item == null) {
				continue;
			}
			else if(item instanceof Versionstamp) {
				if(!((Versionstamp) item).isComplete()) {
					return true;
				}
			}
			else if(item instanceof Tuple) {
				if(((Tuple) item).hasIncompleteVersionstamp()) {
					return true;
				}
			}
			else if(item instanceof Collection<?>) {
				if(hasIncompleteVersionstamp((Collection<?>) item)) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Validates that all strings in a collection (recursing into nested Lists) are well-formed UTF-16.
	 * Tuples are skipped because their strings were already validated at construction time.
	 * @throws IllegalArgumentException if any string is malformed
	 */
	static void validateStrings(Iterable<?> items) {
		for(Object item : items) {
			if(item instanceof String) {
				StringUtil.validate((String)item);
			} else if(item instanceof List<?>) {
				validateStrings((List<?>)item);
			}
			// Tuples: already validated at construction time, skip
		}
	}
}

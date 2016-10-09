package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * This is an utility class to support converting from and to bytes arrays.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class Bytes {

    private static final String DEFAULT_ENCODING = "UTF-8";
    private static final Charset DEFAULT_CHARSET = Charset.forName(DEFAULT_ENCODING);

    public static byte[] empty() {
	return new byte[0];
    }

    public static int putBytes(byte[] destination, byte[] source, int destinationOffset) {
	for (int i = 0; i < source.length; ++i) {
	    destination[destinationOffset + i] = (source[i]);
	}
	return source.length;
    }

    public static byte[] toBytes(boolean b) {
	byte[] bytes = new byte[1];
	bytes[0] = (byte) (b ? 1 : 0);
	return bytes;
    }

    public static int putBytes(byte[] bytes, boolean b, int offset) {
	bytes[offset] = (byte) (b ? 1 : 0);
	return 1;
    }

    public static boolean toBoolean(byte[] bytes) {
	return bytes[0] != 0;
    }

    public static boolean toBoolean(byte[] bytes, int offset) {
	return bytes[offset] != 0;
    }

    public static byte[] toBytes(byte b) {
	byte[] bytes = new byte[1];
	bytes[0] = b;
	return bytes;
    }

    public static int toBytes(byte[] bytes, byte b, int offset) {
	bytes[offset] = b;
	return 1;
    }

    public static int putBytes(byte[] bytes, byte b, int offset) {
	bytes[offset] = b;
	return 1;
    }

    public static byte toByte(byte[] bytes) {
	return bytes[0];
    }

    public static byte toByte(byte[] bytes, int offset) {
	return bytes[offset];
    }

    public static byte[] toBytes(short i) {
	byte[] bytes = new byte[2];
	bytes[1] = (byte) (i);
	bytes[0] = (byte) (i >>> 8);
	return bytes;
    }

    public static int toBytes(byte[] bytes, short s, int offset) {
	bytes[offset + 1] = (byte) s;
	bytes[offset] = (byte) (s >>> 8);
	return 2;
    }

    public static int putBytes(byte[] bytes, short i, int offset) {
	bytes[offset + 1] = (byte) (i);
	bytes[offset] = (byte) (i >>> 8);
	return 2;
    }

    public static short toShort(byte[] bytes) {
	return (short) (((bytes[1] & 0xFF)) //
		| ((bytes[0] & 0xFF) << 8));
    }

    public static short toShort(byte[] bytes, int offset) {
	return (short) (((bytes[offset + 1] & 0xFF)) //
		| ((bytes[offset] & 0xFF) << 8));
    }

    public static byte[] toBytes(int i) {
	byte[] bytes = new byte[4];
	bytes[3] = (byte) (i);
	bytes[2] = (byte) (i >>> 8);
	bytes[1] = (byte) (i >>> 16);
	bytes[0] = (byte) (i >>> 24);
	return bytes;
    }

    public static int toBytes(byte[] bytes, int i, int offset) {
	bytes[offset + 3] = (byte) (i);
	bytes[offset + 2] = (byte) (i >>> 8);
	bytes[offset + 1] = (byte) (i >>> 16);
	bytes[offset] = (byte) (i >>> 24);
	return 4;
    }

    public static int putBytes(byte[] bytes, int i, int offset) {
	bytes[offset + 3] = (byte) (i);
	bytes[offset + 2] = (byte) (i >>> 8);
	bytes[offset + 1] = (byte) (i >>> 16);
	bytes[offset] = (byte) (i >>> 24);
	return 4;
    }

    public static int toInt(byte[] bytes) {
	return ((bytes[3] & 0xFF)) //
		| ((bytes[2] & 0xFF) << 8) //
		| ((bytes[1] & 0xFF) << 16) //
		| ((bytes[0]) << 24);
    }

    public static int toInt(byte[] bytes, int offset) {
	return ((bytes[offset + 3] & 0xFF)) //
		| ((bytes[offset + 2] & 0xFF) << 8) //
		| ((bytes[offset + 1] & 0xFF) << 16) //
		| ((bytes[offset]) << 24);
    }

    public static byte[] toBytes(long l) {
	byte[] bytes = new byte[8];
	bytes[7] = (byte) (l);
	bytes[6] = (byte) (l >>> 8);
	bytes[5] = (byte) (l >>> 16);
	bytes[4] = (byte) (l >>> 24);
	bytes[3] = (byte) (l >>> 32);
	bytes[2] = (byte) (l >>> 40);
	bytes[1] = (byte) (l >>> 48);
	bytes[0] = (byte) (l >>> 56);
	return bytes;
    }

    public static int toBytes(byte[] bytes, long l, int offset) {
	bytes[offset + 7] = (byte) (l);
	bytes[offset + 6] = (byte) (l >>> 8);
	bytes[offset + 5] = (byte) (l >>> 16);
	bytes[offset + 4] = (byte) (l >>> 24);
	bytes[offset + 3] = (byte) (l >>> 32);
	bytes[offset + 2] = (byte) (l >>> 40);
	bytes[offset + 1] = (byte) (l >>> 48);
	bytes[offset] = (byte) (l >>> 56);
	return 8;
    }

    public static long toLong(byte[] bytes) {
	return ((bytes[7] & 0xFFL)) //
		| ((bytes[6] & 0xFFL) << 8) //
		| ((bytes[5] & 0xFFL) << 16) //
		| ((bytes[4] & 0xFFL) << 24) //
		| ((bytes[3] & 0xFFL) << 32) //
		| ((bytes[2] & 0xFFL) << 40) //
		| ((bytes[1] & 0xFFL) << 48) //
		| ((bytes[0]) << 56);
    }

    public static long toLong(byte[] bytes, int offset) {
	return ((bytes[offset + 7] & 0xFFL)) //
		| ((bytes[offset + 6] & 0xFFL) << 8) //
		| ((bytes[offset + 5] & 0xFFL) << 16) //
		| ((bytes[offset + 4] & 0xFFL) << 24) //
		| ((bytes[offset + 3] & 0xFFL) << 32) //
		| ((bytes[offset + 2] & 0xFFL) << 40) //
		| ((bytes[offset + 1] & 0xFFL) << 48) //
		| ((bytes[offset]) << 56);
    }

    public static byte[] toBytes(float f) {
	int bits = Float.floatToIntBits(f);
	byte[] bytes = new byte[4];
	bytes[3] = (byte) (bits & 0xff);
	bytes[2] = (byte) ((bits >> 8) & 0xff);
	bytes[1] = (byte) ((bits >> 16) & 0xff);
	bytes[0] = (byte) ((bits >> 24) & 0xff);
	return bytes;
    }

    public static int toBytes(byte[] bytes, float f, int offset) {
	int bits = Float.floatToIntBits(f);
	bytes[offset + 3] = (byte) (bits & 0xff);
	bytes[offset + 2] = (byte) ((bits >> 8) & 0xff);
	bytes[offset + 1] = (byte) ((bits >> 16) & 0xff);
	bytes[offset] = (byte) ((bits >> 24) & 0xff);
	return 4;
    }

    public static float toFloat(byte[] bytes) {
	return ByteBuffer.wrap(bytes).getFloat();
    }

    public static float toFloat(byte[] bytes, int offset) {
	byte[] b = new byte[] { bytes[offset + 3], bytes[offset + 2], bytes[offset + 1], bytes[offset] };
	return ByteBuffer.wrap(b).getFloat();
    }

    public static byte[] toBytes(double d) {
	long bits = Double.doubleToLongBits(d);
	byte[] bytes = new byte[8];
	bytes[7] = (byte) (bits & 0xff);
	bytes[6] = (byte) ((bits >> 8) & 0xff);
	bytes[5] = (byte) ((bits >> 16) & 0xff);
	bytes[4] = (byte) ((bits >> 24) & 0xff);
	bytes[3] = (byte) ((bits >> 32) & 0xff);
	bytes[2] = (byte) ((bits >> 40) & 0xff);
	bytes[1] = (byte) ((bits >> 48) & 0xff);
	bytes[0] = (byte) ((bits >> 56) & 0xff);
	return bytes;
    }

    public static int toBytes(byte[] bytes, double d, int offset) {
	long bits = Double.doubleToLongBits(d);
	bytes[offset + 7] = (byte) (bits & 0xff);
	bytes[offset + 6] = (byte) ((bits >> 8) & 0xff);
	bytes[offset + 5] = (byte) ((bits >> 16) & 0xff);
	bytes[offset + 4] = (byte) ((bits >> 24) & 0xff);
	bytes[offset + 3] = (byte) ((bits >> 32) & 0xff);
	bytes[offset + 2] = (byte) ((bits >> 40) & 0xff);
	bytes[offset + 1] = (byte) ((bits >> 48) & 0xff);
	bytes[offset] = (byte) ((bits >> 56) & 0xff);
	return 8;
    }

    public static double toDouble(byte[] bytes) {
	return ByteBuffer.wrap(bytes).getDouble();
    }

    public static double toDouble(byte[] bytes, int offset) {
	byte[] b = new byte[] { bytes[offset + 7], bytes[offset + 6], bytes[offset + 5], bytes[offset + 4],
		bytes[offset + 3], bytes[offset + 2], bytes[offset + 1], bytes[offset] };
	return ByteBuffer.wrap(b).getDouble();
    }

    public static byte[] toBytes(String string) {
	return string.getBytes(DEFAULT_CHARSET);
    }

    public static String toString(byte[] bytes) {
	if (bytes == null) {
	    return null;
	}
	return new String(bytes, DEFAULT_CHARSET);
    }

    public static byte[] toBytes(Instant timestamp) {
	long second = timestamp.getEpochSecond();
	int nano = timestamp.getNano();
	byte[] bytes = new byte[12];

	bytes[11] = (byte) (nano);
	bytes[10] = (byte) (nano >>> 8);
	bytes[9] = (byte) (nano >>> 16);
	bytes[8] = (byte) (nano >>> 24);

	bytes[7] = (byte) (second);
	bytes[6] = (byte) (second >>> 8);
	bytes[5] = (byte) (second >>> 16);
	bytes[4] = (byte) (second >>> 24);
	bytes[3] = (byte) (second >>> 32);
	bytes[2] = (byte) (second >>> 40);
	bytes[1] = (byte) (second >>> 48);
	bytes[0] = (byte) (second >>> 56);

	return bytes;
    }

    public static Instant toInstant(byte[] bytes) {
	long seconds = toLong(bytes);
	int nano = toInt(bytes, 8);
	return Instant.ofEpochSecond(seconds, nano);
    }

    public static byte[] toBytes(LocalDate localDate) {
	byte[] bytes = new byte[4];
	short year = (short) localDate.getYear();
	Bytes.toBytes(bytes, year, 0);
	byte month = (byte) localDate.getMonthValue();
	Bytes.toBytes(bytes, month, 2);
	byte day = (byte) localDate.getDayOfMonth();
	Bytes.toBytes(bytes, day, 3);
	return bytes;
    }

    public static LocalDate toLocalDate(byte[] bytes) {
	short year = Bytes.toShort(bytes, 0);
	byte month = Bytes.toByte(bytes, 2);
	byte day = Bytes.toByte(bytes, 3);
	return LocalDate.of(year, month, day);
    }

    public static byte[] toBytes(LocalTime localTime) {
	byte[] bytes = new byte[7];
	byte hour = (byte) localTime.getHour();
	Bytes.toBytes(bytes, hour, 0);
	byte minute = (byte) localTime.getMinute();
	Bytes.toBytes(bytes, minute, 1);
	byte second = (byte) localTime.getSecond();
	Bytes.toBytes(bytes, second, 2);
	int nano = localTime.getNano();
	Bytes.toBytes(bytes, nano, 3);
	return bytes;
    }

    public static LocalTime toLocalTime(byte[] bytes) {
	byte hour = Bytes.toByte(bytes, 0);
	byte minute = Bytes.toByte(bytes, 1);
	byte second = Bytes.toByte(bytes, 2);
	int nano = Bytes.toInt(bytes, 3);
	return LocalTime.of(hour, minute, second, nano);
    }

    public static byte[] toBytes(LocalDateTime localDateTime) {
	byte[] bytes = new byte[11];
	short year = (short) localDateTime.getYear();
	Bytes.toBytes(bytes, year, 0);
	byte month = (byte) localDateTime.getMonthValue();
	Bytes.toBytes(bytes, month, 2);
	byte day = (byte) localDateTime.getDayOfMonth();
	Bytes.toBytes(bytes, day, 3);
	byte hour = (byte) localDateTime.getHour();
	Bytes.toBytes(bytes, hour, 4);
	byte minute = (byte) localDateTime.getMinute();
	Bytes.toBytes(bytes, minute, 5);
	byte second = (byte) localDateTime.getSecond();
	Bytes.toBytes(bytes, second, 6);
	int nano = localDateTime.getNano();
	Bytes.toBytes(bytes, nano, 7);
	return bytes;
    }

    public static LocalDateTime toLocalDateTime(byte[] bytes) {
	short year = Bytes.toShort(bytes, 0);
	byte month = Bytes.toByte(bytes, 2);
	byte day = Bytes.toByte(bytes, 3);
	byte hour = Bytes.toByte(bytes, 4);
	byte minute = Bytes.toByte(bytes, 5);
	byte second = Bytes.toByte(bytes, 6);
	int nano = Bytes.toInt(bytes, 7);
	return LocalDateTime.of(year, month, day, hour, minute, second, nano);
    }

    public static Instant toTombstone(byte[] bytes) {
	boolean isEmpty = true;
	for (byte b : bytes) {
	    if (b != 0) {
		isEmpty = false;
		break;
	    }
	}
	if (isEmpty) {
	    return null;
	}
	boolean isTombstone = false;
	for (byte b : bytes) {
	    if (b > 0) {
		isTombstone = true;
	    }
	}
	return isTombstone ? toInstant(bytes) : null;
    }

    /**
     * This method converts byte array into a readable hex number string.
     * 
     * @param bytes
     * @return
     */
    public static String toHumanReadableString(byte[] bytes) {
	if (bytes == null) {
	    throw new IllegalArgumentException("Byte array must not be null!");
	}
	StringBuilder hexString = new StringBuilder();
	for (int i = 0; i < bytes.length; i++) {
	    if (hexString.length() > 0) {
		hexString.append(' ');
	    }
	    int digit = 0xFF & bytes[i];
	    String hexDigits = Integer.toHexString(digit);
	    if (hexDigits.length() < 2) {
		hexString.append("0");
	    }
	    hexString.append(hexDigits);
	}
	return hexString.toString();
    }

    /**
     * This method converts byte array into a readable hex number string.
     * 
     * @param bytes
     * @return
     */
    public static String toHexString(byte[] bytes) {
	if (bytes == null) {
	    throw new IllegalArgumentException("Byte array must not be null!");
	}
	StringBuilder hexString = new StringBuilder();
	for (int i = 0; i < bytes.length; i++) {
	    int digit = 0xFF & bytes[i];
	    String hexDigits = Integer.toHexString(digit);
	    if (hexDigits.length() < 2) {
		hexString.append("0");
	    }
	    hexString.append(hexDigits);
	}
	return hexString.toString();
    }

    public static byte[] fromHexString(String identifier) {
	if (identifier.length() % 2 != 0) {
	    throw new IllegalArgumentException("The identifier needs to have a even number of digits.");
	}
	byte[] bytes = new byte[identifier.length() / 2];
	for (int i = 0; i < identifier.length() / 2; ++i) {
	    int highHalfByte = char2byte(identifier.charAt(2 * i)) << 4;
	    int lowHalfByte = char2byte(identifier.charAt(2 * i + 1));
	    bytes[i] = (byte) (highHalfByte + lowHalfByte);
	}
	return bytes;
    }

    private static byte char2byte(char c) {
	if (('0' <= c) && ('9' >= c)) {
	    return (byte) (c - '0');
	}
	if (('A' <= c) && ('F' >= c)) {
	    return (byte) (c - 'A' + 10);
	}
	if (('a' <= c) && ('f' >= c)) {
	    return (byte) (c - 'a' + 10);
	}
	throw new IllegalArgumentException("Character '" + c + "' is not part of a hex number.");
    }
}

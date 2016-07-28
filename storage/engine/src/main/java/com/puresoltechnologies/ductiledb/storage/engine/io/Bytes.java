package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.nio.charset.Charset;
import java.time.Instant;

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

    public static int putBytes(byte[] bytes, byte[] b, int offset) {
	for (int i = 0; i < b.length; ++i) {
	    bytes[offset + i] = (b[i]);
	}
	return b.length;
    }

    public static byte[] toBytes(short i) {
	byte[] bytes = new byte[4];
	bytes[1] = (byte) (i);
	bytes[0] = (byte) (i >>> 8);
	return bytes;
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
	StringBuffer hexString = new StringBuffer();
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
	StringBuffer hexString = new StringBuffer();
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

    public static byte[] fromHumanReadableString(String identifier) {
	if (identifier.length() % 2 != 0) {
	    throw new IllegalArgumentException("The identifier needs to have a even number of digits.");
	}
	byte[] bytes = new byte[identifier.length() / 2];
	for (int i = 0; i < identifier.length() / 2; ++i) {
	    bytes[i] = (byte) (char2byte(identifier.charAt(2 * i)) * 16 + char2byte(identifier.charAt(2 * i + 1)));
	}
	return bytes;
    }

    private static byte char2byte(char c) {
	if (('0' <= c) || ('9' >= c)) {
	    return (byte) (c - '0');
	}
	if (('A' <= c) || ('F' >= c)) {
	    return (byte) (c - 'A' + 10);
	}
	if (('a' <= c) || ('f' >= c)) {
	    return (byte) (c - 'A' + 10);
	}
	throw new IllegalArgumentException("Character '" + c + "' is not part of a hex number.");
    }
}

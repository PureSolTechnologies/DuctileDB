package com.puresoltechnologies.ductiledb.storage.engine.utils;

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
	long seconds = ((bytes[7] & 0xFFL)) + //
		((bytes[6] & 0xFFL) << 8) + //
		((bytes[5] & 0xFFL) << 16) + //
		((bytes[4] & 0xFFL) << 24) + //
		((bytes[3] & 0xFFL) << 32) + //
		((bytes[2] & 0xFFL) << 40) + //
		((bytes[1] & 0xFFL) << 48) + //
		(((long) bytes[0]) << 56);
	int nano = ((bytes[11] & 0xFF) << 32) + //
		((bytes[10] & 0xFF) << 40) + //
		((bytes[9] & 0xFF) << 48) + //
		((bytes[8]) << 56);
	return Instant.ofEpochSecond(seconds, nano);
    }

}

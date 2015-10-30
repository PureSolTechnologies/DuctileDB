package com.puresoltechnologies.ductiledb.utils;

/**
 * This class contains utility methods to convert byte arrays into long and vice
 * versa. To support good row ids, byte order is changed to support better
 * partitioning in HBase to avoid hot regions.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class IdEncoder {

    private static final int BITS_PER_BYTE = 8;
    private static final int BYTES_PER_LONG = Long.SIZE / BITS_PER_BYTE;

    public static final byte[] encodeRowId(long id) {
	byte[] encoded = new byte[BYTES_PER_LONG];
	for (int i = 0; i < BYTES_PER_LONG; ++i) {
	    encoded[i] = (byte) (id & 0xff);
	    id >>= BITS_PER_BYTE;
	}
	return encoded;
    }

    public static final long decodeRowId(byte[] id) {
	long decoded = 0;
	for (int i = BYTES_PER_LONG - 1; i >= 0; --i) {
	    decoded <<= BITS_PER_BYTE;
	    decoded = decoded | id[i] & 0xff;
	}
	return decoded;
    }

    public static final void encodeRowId(byte[] target, long id, int startPos) {
	for (int i = 0; i < BYTES_PER_LONG; ++i) {
	    target[startPos + i] = (byte) (id & 0xff);
	    id >>= BITS_PER_BYTE;
	}
    }

    public static final long decodeRowId(byte[] id, int startPos) {
	long decoded = 0;
	for (int i = BYTES_PER_LONG - 1; i >= 0; --i) {
	    decoded <<= BITS_PER_BYTE;
	    decoded = decoded | id[startPos + i] & 0xff;
	}
	return decoded;
    }

}

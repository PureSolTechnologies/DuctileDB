package com.puresoltechnologies.ductiledb.storage.engine.utils;

import java.nio.charset.Charset;

/**
 * This is an utility class to support converting from and to bytes arrays.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class Bytes {

    private static final String DEFAULT_ENCODING = "UTF-8";
    private static final Charset DEFAULT_CHARSET = Charset.forName(DEFAULT_ENCODING);

    public static byte[] toBytes(String string) {
	return string.getBytes(DEFAULT_CHARSET);
    }

    public static String toString(byte[] bytes) {
	if (bytes == null) {
	    return null;
	}
	return new String(bytes, DEFAULT_CHARSET);
    }

    public static byte[] empty() {
	return new byte[0];
    }
}

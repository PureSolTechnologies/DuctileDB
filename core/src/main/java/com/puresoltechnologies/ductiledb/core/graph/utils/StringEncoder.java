package com.puresoltechnologies.ductiledb.core.graph.utils;

import java.nio.charset.Charset;

public class StringEncoder {

    private static final Charset charset = Charset.forName("UTF-8");

    public static byte[] encode(String string) {
	return string.getBytes(charset);
    }

    public static String decode(byte[] bytes) {
	return new String(bytes, charset);
    }

    private StringEncoder() {
    }

}

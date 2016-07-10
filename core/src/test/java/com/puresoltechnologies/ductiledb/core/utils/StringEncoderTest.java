package com.puresoltechnologies.ductiledb.core.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.core.graph.utils.StringEncoder;

public class StringEncoderTest {

    @Test
    public void testCharset() {
	SortedMap<String, Charset> charsets = Charset.availableCharsets();
	for (Entry<String, Charset> entry : charsets.entrySet()) {
	    System.out.println(entry.getKey() + " -> " + entry.getValue());
	}
    }

    @Test
    public void test() {
	byte[] encoded = StringEncoder.encode("StringToBeEncoded!");
	assertNotNull(encoded);
	System.out.println(Arrays.toString(encoded));
	System.out.println(encoded.length);
	String string = StringEncoder.decode(encoded);
	assertNotNull(string);
	assertEquals("StringToBeEncoded!", string);
    }

}

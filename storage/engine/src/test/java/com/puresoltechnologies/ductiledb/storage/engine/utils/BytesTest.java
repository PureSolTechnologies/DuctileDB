package com.puresoltechnologies.ductiledb.storage.engine.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.time.Instant;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class BytesTest {

    @Test
    public void testStringConversion() {
	String string = "TestString";
	byte[] encoded = Bytes.toBytes(string);
	assertNotNull(encoded);
	assertEquals(10, encoded.length);
	String decoded = Bytes.toString(encoded);
	assertEquals(string, decoded);
    }

    @Test
    public void testInstantConversion() {
	Instant now = Instant.now();
	byte[] encoded = Bytes.toBytes(now);
	assertNotNull(encoded);
	assertEquals(12, encoded.length);
	Instant decoded = Bytes.toInstant(encoded);
	assertEquals(now, decoded);
	assertEquals(now.getEpochSecond(), decoded.getEpochSecond());
	assertEquals(now.getNano(), decoded.getNano());
    }

    @Test
    public void testLongConversion() {
	long l = 1;
	byte[] encoded = Bytes.toBytes(l);
	assertEquals(8, encoded.length);
	assertEquals(0, encoded[0]);
	assertEquals(0, encoded[1]);
	assertEquals(0, encoded[2]);
	assertEquals(0, encoded[3]);
	assertEquals(0, encoded[4]);
	assertEquals(0, encoded[5]);
	assertEquals(0, encoded[6]);
	assertEquals(1, encoded[7]);
	long decoded = Bytes.toLong(encoded);
	assertEquals(l, decoded);
    }
}

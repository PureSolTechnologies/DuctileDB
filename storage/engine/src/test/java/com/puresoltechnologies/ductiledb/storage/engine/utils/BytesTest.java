package com.puresoltechnologies.ductiledb.storage.engine.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.time.Instant;

import org.junit.Test;

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

}

package com.puresoltechnologies.ductiledb.utils;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Test;

public class IdEncoderTest {

    @Test
    public void testEncodeAndDecodeSimpleRowId() {
	byte[] encoded = IdEncoder.encodeRowId(1);
	assertEquals(8, encoded.length);
	assertEquals(1, encoded[0]);
	assertEquals(0, encoded[1]);
	assertEquals(0, encoded[2]);
	assertEquals(0, encoded[3]);
	assertEquals(0, encoded[4]);
	assertEquals(0, encoded[5]);
	assertEquals(0, encoded[6]);
	assertEquals(0, encoded[7]);
	long decoded = IdEncoder.decodeRowId(encoded);
	assertEquals(1, decoded);
    }

    @Test
    public void testEncodeAndDecodeSimpleRowId2() {
	byte[] encoded = IdEncoder.encodeRowId(513);
	assertEquals(8, encoded.length);
	assertEquals(1, encoded[0]);
	assertEquals(2, encoded[1]);
	assertEquals(0, encoded[2]);
	assertEquals(0, encoded[3]);
	assertEquals(0, encoded[4]);
	assertEquals(0, encoded[5]);
	assertEquals(0, encoded[6]);
	assertEquals(0, encoded[7]);
	long decoded = IdEncoder.decodeRowId(encoded);
	assertEquals(513, decoded);
    }

    @Test
    public void testEncodeAndDecodeSimpleRowId3() {
	byte[] encoded = IdEncoder.encodeRowId(128);
	assertEquals(8, encoded.length);
	assertEquals(-128, encoded[0]);
	assertEquals(0, encoded[1]);
	assertEquals(0, encoded[2]);
	assertEquals(0, encoded[3]);
	assertEquals(0, encoded[4]);
	assertEquals(0, encoded[5]);
	assertEquals(0, encoded[6]);
	assertEquals(0, encoded[7]);
	long decoded = IdEncoder.decodeRowId(encoded);
	assertEquals(128, decoded);
    }

    @Test
    public void testEncodeAndDecodeRandomRowId() {
	Random random = new Random();
	for (int i = 0; i < 1000; i++) {
	    long id = random.nextLong();
	    byte[] encoded = IdEncoder.encodeRowId(id);
	    assertEquals(8, encoded.length);
	    long decoded = IdEncoder.decodeRowId(encoded);
	    assertEquals(id, decoded);
	}
    }

    @Test
    public void testEncodeAndDecodeSequentialRowIds() {
	for (long id = 0; id < 10000; id++) {
	    byte[] encoded = IdEncoder.encodeRowId(id);
	    assertEquals(8, encoded.length);
	    long decoded = IdEncoder.decodeRowId(encoded);
	    assertEquals(id, decoded);
	}
    }

}

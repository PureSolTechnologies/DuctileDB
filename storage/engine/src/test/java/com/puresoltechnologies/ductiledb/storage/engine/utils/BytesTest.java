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

    @Test
    public void testEncodeAndDecodeOfString() {
	String string = "TestString!";
	byte[] bytes = Bytes.toBytes(string);
	assertEquals(string, Bytes.toString(bytes));
    }

    @Test
    public void testEncodeAndDecodeOfHexString() {
	String string = "00010203040506070809101112131415fdfeff";
	byte[] bytes = Bytes.fromHexString(string);
	assertEquals(0x00, bytes[0]);
	assertEquals(0x01, bytes[1]);
	assertEquals(0x02, bytes[2]);
	assertEquals(0x03, bytes[3]);
	assertEquals(0x04, bytes[4]);
	assertEquals(0x05, bytes[5]);
	assertEquals(0x06, bytes[6]);
	assertEquals(0x07, bytes[7]);
	assertEquals(0x08, bytes[8]);
	assertEquals(0x09, bytes[9]);
	assertEquals(0x10, bytes[10]);
	assertEquals(0x11, bytes[11]);
	assertEquals(0x12, bytes[12]);
	assertEquals(0x13, bytes[13]);
	assertEquals(0x14, bytes[14]);
	assertEquals(0x15, bytes[15]);
	assertEquals(0xfd, 0xff & bytes[16]);
	assertEquals(0xfe, 0xff & bytes[17]);
	assertEquals(0xff, 0xff & bytes[18]);
	assertEquals(string, Bytes.toHexString(bytes));
    }
}

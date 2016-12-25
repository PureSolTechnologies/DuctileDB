package com.puresoltechnologies.ductiledb.logstore.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.logstore.utils.Bytes;

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
    public void testLongConversionBigEndian() {
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

    @Test
    public void testBoolean() {
	boolean b = true;
	byte[] bytes = Bytes.toBytes(b);
	assertEquals(b, Bytes.toBoolean(bytes));

	b = false;
	bytes = Bytes.toBytes(b);
	assertEquals(b, Bytes.toBoolean(bytes));
    }

    @Test
    public void testByte() {
	byte b = 123;
	byte[] bytes = Bytes.toBytes(b);
	assertEquals(b, Bytes.toByte(bytes));
    }

    @Test
    public void testShort() {
	short b = 32_000;
	byte[] bytes = Bytes.toBytes(b);
	assertEquals(b, Bytes.toShort(bytes));
    }

    @Test
    public void testInt() {
	int b = 1_000_000_000;
	byte[] bytes = Bytes.toBytes(b);
	assertEquals(b, Bytes.toInt(bytes));
    }

    @Test
    public void testLong() {
	long b = 1_000_000_000_000l;
	byte[] bytes = Bytes.toBytes(b);
	assertEquals(b, Bytes.toLong(bytes));
    }

    @Test
    public void testFloat() {
	float b = (float) 1.23456e4;
	byte[] bytes = Bytes.toBytes(b);
	assertEquals(b, Bytes.toFloat(bytes), 0.0);
    }

    @Test
    public void testDouble() {
	double b = 1.23456e4;
	byte[] bytes = Bytes.toBytes(b);
	assertEquals(b, Bytes.toDouble(bytes), 0.0);
    }

    @Test
    public void testLocalTime() {
	LocalTime localTime = LocalTime.of(11, 12, 13, 123456789);
	byte[] bytes = Bytes.toBytes(localTime);
	assertEquals(localTime, Bytes.toLocalTime(bytes));
    }

    @Test
    public void testLocalDate() {
	LocalDate localDate = LocalDate.of(2016, 10, 9);
	byte[] bytes = Bytes.toBytes(localDate);
	assertEquals(localDate, Bytes.toLocalDate(bytes));
    }

    @Test
    public void testLocalDateTime() {
	LocalDateTime localDateTime = LocalDateTime.of(2016, 10, 9, 11, 12, 13, 123456789);
	byte[] bytes = Bytes.toBytes(localDateTime);
	assertEquals(localDateTime, Bytes.toLocalDateTime(bytes));
    }
}

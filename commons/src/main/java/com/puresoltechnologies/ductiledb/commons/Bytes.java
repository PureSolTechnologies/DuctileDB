package com.puresoltechnologies.ductiledb.commons;

import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import com.puresoltechnologies.streaming.binary.BigEndianBytes;

public class Bytes {

    // XXX charset needs to be configurable?
    private static final Charset defaultCharset = Charset.defaultCharset();
    private static final com.puresoltechnologies.streaming.binary.Bytes converter = new BigEndianBytes();

    public static ByteOrder getByteOrder() {
	return converter.getByteOrder();
    }

    public static int putBytes(byte[] destination, int destinationOffset, byte[] source) {
	return converter.putBytes(destination, destinationOffset, source);
    }

    public static byte[] fromBoolean(boolean b) {
	return converter.fromBoolean(b);
    }

    public static int putBoolean(byte[] bytes, boolean b, int offset) {
	return converter.putBoolean(bytes, b, offset);
    }

    public static boolean toBoolean(byte[] bytes) {
	return converter.toBoolean(bytes);
    }

    public static boolean toBoolean(byte[] bytes, int offset) {
	return converter.toBoolean(bytes, offset);
    }

    public static byte[] fromByte(byte b) {
	return converter.fromByte(b);
    }

    public static int putByte(byte[] bytes, byte b, int offset) {
	return converter.putByte(bytes, b, offset);
    }

    public static byte toByte(byte[] bytes) {
	return converter.toByte(bytes);
    }

    public static byte toByte(byte[] bytes, int offset) {
	return converter.toByte(bytes, offset);
    }

    public static byte[] fromUnsignedByte(int b) {
	return converter.fromUnsignedByte(b);
    }

    public static int putUnsignedByte(byte[] bytes, int b, int offset) {
	return converter.putUnsignedByte(bytes, b, offset);
    }

    public static int toUnsignedByte(byte[] bytes) {
	return converter.toUnsignedByte(bytes);
    }

    public static int toUnsignedByte(byte[] bytes, int offset) {
	return converter.toUnsignedByte(bytes, offset);
    }

    public static byte[] fromShort(short i) {
	return converter.fromShort(i);
    }

    public static int putShort(byte[] bytes, short s, int offset) {
	return converter.putShort(bytes, s, offset);
    }

    public static short toShort(byte[] bytes) {
	return converter.toShort(bytes);
    }

    public static short toShort(byte[] bytes, int offset) {
	return converter.toShort(bytes, offset);
    }

    public static byte[] fromUnsignedShort(int i) {
	return converter.fromUnsignedShort(i);
    }

    public static int putUnsignedShort(byte[] bytes, int s, int offset) {
	return converter.putUnsignedShort(bytes, s, offset);
    }

    public static int toUnsignedShort(byte[] bytes) {
	return converter.toUnsignedShort(bytes);
    }

    public static int toUnsignedShort(byte[] bytes, int offset) {
	return converter.toUnsignedShort(bytes, offset);
    }

    public static byte[] fromInt(int i) {
	return converter.fromInt(i);
    }

    public static int putInt(byte[] bytes, int i, int offset) {
	return converter.putInt(bytes, i, offset);
    }

    public static int toInt(byte[] bytes) {
	return converter.toInt(bytes);
    }

    public static int toInt(byte[] bytes, int offset) {
	return converter.toInt(bytes, offset);
    }

    public static byte[] fromUnsignedInt(long i) {
	return converter.fromUnsignedInt(i);
    }

    public static int putUnsignedInt(byte[] bytes, long i, int offset) {
	return converter.putUnsignedInt(bytes, i, offset);
    }

    public static long toUnsignedInt(byte[] bytes) {
	return converter.toUnsignedInt(bytes);
    }

    public static long toUnsignedInt(byte[] bytes, int offset) {
	return converter.toUnsignedInt(bytes, offset);
    }

    public static byte[] fromLong(long l) {
	return converter.fromLong(l);
    }

    public static int putLong(byte[] bytes, long l, int offset) {
	return converter.putLong(bytes, l, offset);
    }

    public static long toLong(byte[] bytes) {
	return converter.toLong(bytes);
    }

    public static long toLong(byte[] bytes, int offset) {
	return converter.toLong(bytes, offset);
    }

    public static byte[] fromFloat(float f) {
	return converter.fromFloat(f);
    }

    public static int putFloat(byte[] bytes, float f, int offset) {
	return converter.putFloat(bytes, f, offset);
    }

    public static float toFloat(byte[] bytes) {
	return converter.toFloat(bytes);
    }

    public static float toFloat(byte[] bytes, int offset) {
	return converter.toFloat(bytes, offset);
    }

    public static byte[] fromDouble(double d) {
	return converter.fromDouble(d);
    }

    public static int putDouble(byte[] bytes, double d, int offset) {
	return converter.putDouble(bytes, d, offset);
    }

    public static double toDouble(byte[] bytes) {
	return converter.toDouble(bytes);
    }

    public static double toDouble(byte[] bytes, int offset) {
	return converter.toDouble(bytes, offset);
    }

    public static byte[] fromString(String string) {
	return converter.fromString(string, defaultCharset);
    }

    public static String toString(byte[] bytes) {
	return converter.toString(bytes, defaultCharset);
    }

    public static String toHumanReadableString(byte[] bytes) {
	return converter.toHumanReadableString(bytes);
    }

    public static String toHexString(byte[] bytes) {
	return converter.toHexString(bytes);
    }

    public static byte[] fromHexString(String identifier) {
	return converter.fromHexString(identifier);
    }

    public static byte[] fromInstant(Instant timestamp) {
	return converter.fromInstant(timestamp);
    }

    public static Instant toInstant(byte[] bytes) {
	return converter.toInstant(bytes);
    }

    public static byte[] fromLocalDate(LocalDate localDate) {
	return converter.fromLocalDate(localDate);
    }

    public static LocalDate toLocalDate(byte[] bytes) {
	return converter.toLocalDate(bytes);
    }

    public static byte[] fromLocalTime(LocalTime localTime) {
	return converter.fromLocalTime(localTime);
    }

    public static LocalTime toLocalTime(byte[] bytes) {
	return converter.toLocalTime(bytes);
    }

    public static byte[] fromLocalDateTime(LocalDateTime localDateTime) {
	return converter.fromLocalDateTime(localDateTime);
    }

    public static LocalDateTime toLocalDateTime(byte[] bytes) {
	return converter.toLocalDateTime(bytes);
    }

    public static Instant toTombstone(byte[] bytes) {
	return converter.toTombstone(bytes);
    }

    public static byte[] empty() {
	return new byte[] {};
    }

    /**
     * Private constructor to avoid instantiation.
     */
    private Bytes() {
    }
}

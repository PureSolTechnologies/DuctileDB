package com.puresoltechnologies.ductiledb.storage.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class CompoundKeyTest {

    @Test(expected = NullPointerException.class)
    public void testNullKeyParts() {
	CompoundKey.encode((byte[][]) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyKeyParts() {
	CompoundKey.encode();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooManyKeyParts() {
	byte[][] keyParts = new byte[256][];
	for (int i = 0; i < 256; ++i) {
	    keyParts[i] = new byte[] { (byte) i };
	}
	CompoundKey.encode(keyParts);
    }

    @Test
    public void testSingleKeyPart() {
	CompoundKey compoundKey = CompoundKey.create(new byte[] { 42 });
	assertNotNull(compoundKey);
	assertEquals(1, compoundKey.getPartNum());
	byte[] key = compoundKey.getBytes();
	assertNotNull(key);
	assertEquals(6, key.length);
	assertEquals(1, key[0]);
	assertEquals(0, key[1]);
	assertEquals(0, key[2]);
	assertEquals(0, key[3]);
	assertEquals(1, key[4]);
	assertEquals(42, key[5]);
    }

    @Test
    public void testMultiKeyParts() {
	CompoundKey compoundKey = CompoundKey.create(new byte[] { 42 }, new byte[] { 21, 22 },
		new byte[] { 13, 14, 15 });
	assertNotNull(compoundKey);
	assertEquals(3, compoundKey.getPartNum());
	byte[] key = compoundKey.getBytes();
	assertNotNull(key);
	assertEquals(19, key.length);
	assertEquals(3, key[0]);
	assertEquals(0, key[1]);
	assertEquals(0, key[2]);
	assertEquals(0, key[3]);
	assertEquals(1, key[4]);
	assertEquals(42, key[5]);
	assertEquals(0, key[6]);
	assertEquals(0, key[7]);
	assertEquals(0, key[8]);
	assertEquals(2, key[9]);
	assertEquals(21, key[10]);
	assertEquals(22, key[11]);
	assertEquals(0, key[12]);
	assertEquals(0, key[13]);
	assertEquals(0, key[14]);
	assertEquals(3, key[15]);
	assertEquals(13, key[16]);
	assertEquals(14, key[17]);
	assertEquals(15, key[18]);
    }

    @Test
    public void testPartNumEncodingToByte() {
	byte[][] keyParts = new byte[200][];
	for (int i = 0; i < 200; ++i) {
	    keyParts[i] = new byte[] { (byte) i };
	}

	CompoundKey compoundKey = CompoundKey.create(keyParts);
	assertNotNull(compoundKey);
	assertEquals(200, compoundKey.getPartNum());
	byte[] key = compoundKey.getBytes();
	assertNotNull(key);
	assertEquals(1001, key.length);
	assertEquals(200, key[0] & 0xFF);
	for (int i = 0; i < 200; ++i) {
	    assertEquals(0, key[5 * i + 1]);
	    assertEquals(0, key[5 * i + 2]);
	    assertEquals(0, key[5 * i + 3]);
	    assertEquals(1, key[5 * i + 4]);
	    assertEquals(i, key[5 * i + 5] & 0xFF);
	}
    }

    @Test
    public void testDecode() {
	byte[][] keyParts = CompoundKey
		.decode(new byte[] { 3, 0, 0, 0, 1, 42, 0, 0, 0, 2, 12, 13, 0, 0, 0, 3, 1, 2, 3 });
	assertEquals(3, keyParts.length);
	assertEquals(1, keyParts[0].length);
	assertEquals(42, keyParts[0][0]);
	assertEquals(2, keyParts[1].length);
	assertEquals(12, keyParts[1][0]);
	assertEquals(13, keyParts[1][1]);
	assertEquals(3, keyParts[2].length);
	assertEquals(1, keyParts[2][0]);
	assertEquals(2, keyParts[2][1]);
	assertEquals(3, keyParts[2][2]);
    }
}

package com.puresoltechnologies.ductiledb.core;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.api.EdgeDirection;

public class EdgeKeyTest {

    @Test
    public void testEncodeINDirection() {
	EdgeKey key = new EdgeKey(EdgeDirection.IN, 1, 2, "A");
	byte[] encoded = key.encode();
	assertEquals(18, encoded.length);
	assertEquals(0, encoded[0]);
	assertEquals(1, encoded[1]);
	assertEquals(0, encoded[2]);
	assertEquals(0, encoded[3]);
	assertEquals(0, encoded[4]);
	assertEquals(0, encoded[5]);
	assertEquals(0, encoded[6]);
	assertEquals(0, encoded[7]);
	assertEquals(0, encoded[8]);
	assertEquals(2, encoded[9]);
	assertEquals(0, encoded[10]);
	assertEquals(0, encoded[11]);
	assertEquals(0, encoded[12]);
	assertEquals(0, encoded[13]);
	assertEquals(0, encoded[14]);
	assertEquals(0, encoded[15]);
	assertEquals(0, encoded[16]);
	assertEquals(65, encoded[17]);
	EdgeKey decoded = EdgeKey.decode(encoded);
	assertEquals(key, decoded);
    }

    @Test
    public void testEncodeOUTDirection() {
	EdgeKey key = new EdgeKey(EdgeDirection.OUT, 128, 513, "ABC");
	byte[] encoded = key.encode();
	assertEquals(20, encoded.length);
	assertEquals(1, encoded[0]);
	assertEquals(-128, encoded[1]);
	assertEquals(0, encoded[2]);
	assertEquals(0, encoded[3]);
	assertEquals(0, encoded[4]);
	assertEquals(0, encoded[5]);
	assertEquals(0, encoded[6]);
	assertEquals(0, encoded[7]);
	assertEquals(0, encoded[8]);
	assertEquals(1, encoded[9]);
	assertEquals(2, encoded[10]);
	assertEquals(0, encoded[11]);
	assertEquals(0, encoded[12]);
	assertEquals(0, encoded[13]);
	assertEquals(0, encoded[14]);
	assertEquals(0, encoded[15]);
	assertEquals(0, encoded[16]);
	assertEquals(65, encoded[17]);
	assertEquals(66, encoded[18]);
	assertEquals(67, encoded[19]);
	EdgeKey decoded = EdgeKey.decode(encoded);
	assertEquals(key, decoded);
    }

}

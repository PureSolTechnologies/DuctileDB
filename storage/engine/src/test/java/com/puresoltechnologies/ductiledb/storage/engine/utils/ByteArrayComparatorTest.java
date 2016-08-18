package com.puresoltechnologies.ductiledb.storage.engine.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class ByteArrayComparatorTest {

    private static final ByteArrayComparator comparator = ByteArrayComparator.getInstance();

    @Test
    public void testEquals() {
	assertEquals(0, comparator.compare(new byte[] {}, new byte[] {}));
	assertEquals(0, comparator.compare(new byte[] { 1 }, new byte[] { 1 }));
	assertEquals(0, comparator.compare(new byte[] { 1, 1, 1 }, new byte[] { 1, 1, 1 }));
    }

    @Test
    public void testSmaller() {
	assertEquals(-1, comparator.compare(new byte[] {}, new byte[] { 1 }));
	assertEquals(-1, comparator.compare(new byte[] { 1 }, new byte[] { 1, 1 }));
	assertEquals(-1, comparator.compare(new byte[] { 1, 1 }, new byte[] { 1, 1, 1 }));

	assertEquals(-1, comparator.compare(new byte[] { 1 }, new byte[] { 2 }));
	assertEquals(-1, comparator.compare(new byte[] { 1, 1 }, new byte[] { 1, 2 }));
	assertEquals(-1, comparator.compare(new byte[] { 1, 1, 1 }, new byte[] { 1, 1, 2 }));
    }

    @Test
    public void testGreater() {
	assertEquals(1, comparator.compare(new byte[] { 1 }, new byte[] {}));
	assertEquals(1, comparator.compare(new byte[] { 1, 1 }, new byte[] { 1 }));
	assertEquals(1, comparator.compare(new byte[] { 1, 1, 1 }, new byte[] { 1, 1 }));

	assertEquals(1, comparator.compare(new byte[] { 2 }, new byte[] { 1 }));
	assertEquals(1, comparator.compare(new byte[] { 1, 2 }, new byte[] { 1, 1 }));
	assertEquals(1, comparator.compare(new byte[] { 1, 1, 2 }, new byte[] { 1, 1, 1 }));
    }

    @Test
    public void testConvertedLongs() {
	assertEquals(0, comparator.compare(Bytes.toBytes(1l), Bytes.toBytes(1l)));
	assertEquals(-1, comparator.compare(Bytes.toBytes(1l), Bytes.toBytes(2l)));
	assertEquals(1, comparator.compare(Bytes.toBytes(2l), Bytes.toBytes(1l)));

	for (long l1 = 0; l1 <= 1024; ++l1) {
	    for (long l2 = 0; l2 <= 1024; ++l2) {
		if (l1 == l2) {
		    assertEquals("Invalid comparison result for l1=" + l1 + " and l2=" + l2, 0,
			    comparator.compare(Bytes.toBytes(l1), Bytes.toBytes(l2)));
		} else if (l1 < l2) {
		    assertTrue("Invalid comparison result for l1=" + l1 + " and l2=" + l2,
			    comparator.compare(Bytes.toBytes(l1), Bytes.toBytes(l2)) < 0);
		} else {
		    assertTrue("Invalid comparison result for l1=" + l1 + " and l2=" + l2,
			    comparator.compare(Bytes.toBytes(l1), Bytes.toBytes(l2)) > 0);
		}
	    }
	}
    }

}

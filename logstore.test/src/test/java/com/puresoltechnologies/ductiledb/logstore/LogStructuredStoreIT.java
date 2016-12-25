package com.puresoltechnologies.ductiledb.logstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.logstore.utils.ByteArrayComparator;

public class LogStructuredStoreIT extends AbstractLogStructuredStoreTest {

    @Test
    public void testSimpleCRUD() {
	Key rowKey = Key.of(1l);
	byte[] value = new byte[] { 1, 2, 3 };
	byte[] value2 = new byte[] { 1, 2, 3, 4 };

	LogStructuredStore store = getStore();
	// Not presented, yet.
	byte[] readValue = store.get(rowKey);
	assertNull(readValue);
	// Put value.
	store.put(rowKey, value);
	// Check value.
	readValue = store.get(rowKey);
	assertNotNull(readValue);
	assertEquals(0, ByteArrayComparator.compareArrays(value, readValue));
	// Put value2.
	store.put(rowKey, value2);
	// Check value2.
	readValue = store.get(rowKey);
	assertNotNull(readValue);
	assertEquals(0, ByteArrayComparator.compareArrays(value2, readValue));
	// Delete row.
	store.delete(rowKey);
	// Check deletion.
	readValue = store.get(rowKey);
	assertNull(readValue);
    }

}

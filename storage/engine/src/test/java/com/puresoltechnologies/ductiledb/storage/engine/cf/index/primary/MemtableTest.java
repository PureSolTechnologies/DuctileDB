package com.puresoltechnologies.ductiledb.storage.engine.cf.index.primary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.time.Duration;

import org.junit.Test;

import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class MemtableTest {

    @Test
    public void testCRUD() {
	Memtable memtable = new Memtable();
	// Check behavior of empty Memtable
	IndexEntry entry = memtable.get(new Key(Bytes.toBytes(0l)));
	assertNull(entry);
	memtable.delete(new Key(Bytes.toBytes(0l)));
	assertNull(entry);
	assertEquals(0, memtable.size());
	// Check put
	memtable.put(new IndexEntry(new Key(Bytes.toBytes(0l)), new File("file"), 123l));
	assertEquals(1, memtable.size());
	entry = memtable.get(new Key(Bytes.toBytes(0l)));
	assertNotNull(entry);
	assertEquals(new IndexEntry(new Key(Bytes.toBytes(0l)), new File("file"), 123l), entry);
	// Check put of new value does not change former
	memtable.put(new IndexEntry(new Key(Bytes.toBytes(1l)), new File("file2"), 1234l));
	assertEquals(2, memtable.size());
	entry = memtable.get(new Key(Bytes.toBytes(0l)));
	assertNotNull(entry);
	assertEquals(new IndexEntry(new Key(Bytes.toBytes(0l)), new File("file"), 123l), entry);
	entry = memtable.get(new Key(Bytes.toBytes(1l)));
	assertNotNull(entry);
	assertEquals(new IndexEntry(new Key(Bytes.toBytes(1l)), new File("file2"), 1234l), entry);
	// Check put
	memtable.put(new IndexEntry(new Key(Bytes.toBytes(0l)), new File("file3"), 12345l));
	assertEquals(2, memtable.size());
	entry = memtable.get(new Key(Bytes.toBytes(0l)));
	assertNotNull(entry);
	assertEquals(new IndexEntry(new Key(Bytes.toBytes(0l)), new File("file3"), 12345l), entry);
	entry = memtable.get(new Key(Bytes.toBytes(1l)));
	assertNotNull(entry);
	assertEquals(new IndexEntry(new Key(Bytes.toBytes(1l)), new File("file2"), 1234l), entry);
	// Check delete
	memtable.delete(new Key(Bytes.toBytes(0l)));
	assertEquals(1, memtable.size());
	entry = memtable.get(new Key(Bytes.toBytes(0l)));
	assertNull(entry);
	entry = memtable.get(new Key(Bytes.toBytes(1l)));
	assertNotNull(entry);
	assertEquals(new IndexEntry(new Key(Bytes.toBytes(1l)), new File("file2"), 1234l), entry);
    }

    @Test
    public void testSimplePerformance() {
	Memtable memtable = new Memtable();
	File file = new File("file");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	for (int i = 0; i < 10000; ++i) {
	    memtable.put(new IndexEntry(new Key(Bytes.toBytes((long) i)), file, i));
	}
	stopWatch.stop();
	System.out.println(stopWatch);
	assertTrue(stopWatch.getDuration().compareTo(Duration.ofSeconds(1, 100)) < 0);
    }

    @Test
    public void testRangeIterator() {
	Memtable memtable = new Memtable();
	File file = new File("file");
	for (int i = 0; i < 500; ++i) {
	    memtable.put(new IndexEntry(new Key(Bytes.toBytes((long) (2 * i))), file, 2 * i));
	}
	IndexIterator iterator = memtable.iterator(new Key(Bytes.toBytes(100l)), new Key(Bytes.toBytes(400l)));
	long expected = 100l;
	while (iterator.hasNext()) {
	    byte[] key = iterator.next().getRowKey().getKey();
	    assertEquals(expected, Bytes.toLong(key));
	    expected += 2;
	}
	assertEquals(402, expected);
	assertFalse(iterator.hasNext());
    }
}

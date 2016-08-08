package com.puresoltechnologies.ductiledb.storage.engine.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.time.Duration;

import org.junit.Test;

import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class MemtableTest {

    @Test
    public void testCRUD() {
	Memtable memtable = new Memtable();
	// Check behavior of empty Memtable
	IndexEntry entry = memtable.get(new RowKey(Bytes.toBytes(0l)));
	assertNull(entry);
	memtable.delete(new RowKey(Bytes.toBytes(0l)));
	assertNull(entry);
	assertEquals(0, memtable.size());
	// Check put
	memtable.put(new IndexEntry(new RowKey(Bytes.toBytes(0l)), new File("file"), 123l));
	assertEquals(1, memtable.size());
	entry = memtable.get(new RowKey(Bytes.toBytes(0l)));
	assertNotNull(entry);
	assertEquals(new IndexEntry(new RowKey(Bytes.toBytes(0l)), new File("file"), 123l), entry);
	// Check put of new value does not change former
	memtable.put(new IndexEntry(new RowKey(Bytes.toBytes(1l)), new File("file2"), 1234l));
	assertEquals(2, memtable.size());
	entry = memtable.get(new RowKey(Bytes.toBytes(0l)));
	assertNotNull(entry);
	assertEquals(new IndexEntry(new RowKey(Bytes.toBytes(0l)), new File("file"), 123l), entry);
	entry = memtable.get(new RowKey(Bytes.toBytes(1l)));
	assertNotNull(entry);
	assertEquals(new IndexEntry(new RowKey(Bytes.toBytes(1l)), new File("file2"), 1234l), entry);
	// Check put
	memtable.put(new IndexEntry(new RowKey(Bytes.toBytes(0l)), new File("file3"), 12345l));
	assertEquals(2, memtable.size());
	entry = memtable.get(new RowKey(Bytes.toBytes(0l)));
	assertNotNull(entry);
	assertEquals(new IndexEntry(new RowKey(Bytes.toBytes(0l)), new File("file3"), 12345l), entry);
	entry = memtable.get(new RowKey(Bytes.toBytes(1l)));
	assertNotNull(entry);
	assertEquals(new IndexEntry(new RowKey(Bytes.toBytes(1l)), new File("file2"), 1234l), entry);
	// Check delete
	memtable.delete(new RowKey(Bytes.toBytes(0l)));
	assertEquals(1, memtable.size());
	entry = memtable.get(new RowKey(Bytes.toBytes(0l)));
	assertNull(entry);
	entry = memtable.get(new RowKey(Bytes.toBytes(1l)));
	assertNotNull(entry);
	assertEquals(new IndexEntry(new RowKey(Bytes.toBytes(1l)), new File("file2"), 1234l), entry);
    }

    @Test
    public void testSimplePerformance() {
	Memtable memtable = new Memtable();
	File file = new File("file");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	for (int i = 0; i < 10000; ++i) {
	    memtable.put(new IndexEntry(new RowKey(Bytes.toBytes((long) i)), file, i));
	}
	stopWatch.stop();
	System.out.println(stopWatch);
	assertTrue(stopWatch.getDuration().compareTo(Duration.ofSeconds(1, 100)) < 0);
    }
}

package com.puresoltechnologies.ductiledb.logstore.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.logstore.Key;

public class MemtableTest {

    private static final int THREAD_POOL_SIZE = 16;
    private static final int NUMBER_OF_VALUES = 512;

    @Test
    public void testCRUD() {
	Memtable memtable = new Memtable();
	// Check behavior of empty Memtable
	IndexEntry entry = memtable.get(Key.of(0l));
	assertNull(entry);
	memtable.delete(Key.of(0l));
	assertNull(entry);
	assertEquals(0, memtable.size());
	// Check put
	memtable.put(new IndexEntry(Key.of(0l), new File("file"), 123l));
	assertEquals(1, memtable.size());
	entry = memtable.get(Key.of(0l));
	assertNotNull(entry);
	assertEquals(new IndexEntry(Key.of(0l), new File("file"), 123l), entry);
	// Check put of new value does not change former
	memtable.put(new IndexEntry(Key.of(1l), new File("file2"), 1234l));
	assertEquals(2, memtable.size());
	entry = memtable.get(Key.of(0l));
	assertNotNull(entry);
	assertEquals(new IndexEntry(Key.of(0l), new File("file"), 123l), entry);
	entry = memtable.get(Key.of(1l));
	assertNotNull(entry);
	assertEquals(new IndexEntry(Key.of(1l), new File("file2"), 1234l), entry);
	// Check put
	memtable.put(new IndexEntry(Key.of(0l), new File("file3"), 12345l));
	assertEquals(2, memtable.size());
	entry = memtable.get(Key.of(0l));
	assertNotNull(entry);
	assertEquals(new IndexEntry(Key.of(0l), new File("file3"), 12345l), entry);
	entry = memtable.get(Key.of(1l));
	assertNotNull(entry);
	assertEquals(new IndexEntry(Key.of(1l), new File("file2"), 1234l), entry);
	// Check delete
	memtable.delete(Key.of(0l));
	assertEquals(1, memtable.size());
	entry = memtable.get(Key.of(0l));
	assertNull(entry);
	entry = memtable.get(Key.of(1l));
	assertNotNull(entry);
	assertEquals(new IndexEntry(Key.of(1l), new File("file2"), 1234l), entry);
    }

    @Test
    public void testSimplePerformance() {
	Memtable memtable = new Memtable();
	File file = new File("file");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	for (int i = 0; i < 10000; ++i) {
	    memtable.put(new IndexEntry(Key.of((long) i), file, i));
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
	    memtable.put(new IndexEntry(Key.of((long) (2 * i)), file, 2 * i));
	}
	IndexEntryIterator iterator = memtable.iterator(Key.of(100l), Key.of(400l));
	long expected = 100l;
	while (iterator.hasNext()) {
	    byte[] key = iterator.next().getRowKey().getBytes();
	    assertEquals(expected, Bytes.toLong(key));
	    expected += 2;
	}
	assertEquals(402, expected);
	assertFalse(iterator.hasNext());
    }

    @Test
    public void testConcurrency() throws InterruptedException, ExecutionException {
	Memtable memtable = new Memtable();
	long start = System.nanoTime();
	ExecutorService fixedThreadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
	Map<Integer, Future<Void>> futures = new HashMap<>();
	for (int threadId = 0; threadId < THREAD_POOL_SIZE; threadId++) {
	    final int id = threadId;
	    Callable<Void> callable = new Callable<Void>() {
		@Override
		public Void call() throws Exception {
		    final int offset = id * NUMBER_OF_VALUES;
		    File dataFile = new File("file" + id);
		    for (int valueId = 0; valueId < NUMBER_OF_VALUES; valueId++) {
			Key rowKey = Key.of(offset + valueId);
			long value = offset + valueId;

			// Not presented, yet.
			IndexEntry readValue = memtable.get(rowKey);
			assertNull(readValue);
			// Put value.
			IndexEntry indexEntry = new IndexEntry(rowKey, dataFile, value);
			memtable.put(indexEntry);
			// Check value.
			readValue = memtable.get(rowKey);
			assertNotNull(readValue);
			assertEquals(indexEntry, readValue);
			// Delete row.
			memtable.delete(rowKey);
			// Check deletion.
			readValue = memtable.get(rowKey);
			assertNull(readValue);
		    }
		    return null;
		}
	    };
	    Future<Void> future = fixedThreadPool.submit(callable);
	    futures.put(threadId, future);
	}
	fixedThreadPool.shutdown();
	fixedThreadPool.awaitTermination(60, TimeUnit.SECONDS);
	for (

	Future<Void> future : futures.values()) {
	    future.get();
	}
	long end = System.nanoTime();
	int millis = (int) ((end - start) / 1000000);
	System.out.println("time: " + millis + "ms");
	System.out.println("CRUD/s: " + (THREAD_POOL_SIZE * NUMBER_OF_VALUES) / (millis / 1000.0) + " CRUD/s");
    }
}

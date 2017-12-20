package com.puresoltechnologies.ductiledb.logstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.logstore.utils.ByteArrayComparator;

public class LogStructuredStoreConcurrencyIT extends AbstractLogStructuredStoreTest {

    private static final int THREAD_POOL_SIZE = 64;
    private static final int NUMBER_OF_VALUES = 1000;

    private final LogStructuredStore store = getStore();

    @Test
    public void testSimpleConcurrencyCRUDonSameKeyPerThread() throws InterruptedException, ExecutionException {
	Counter compactionCounter = (Counter) store.getMetric(LogStructuredStoreMetric.COMPACTION_COUNTER);
	assertNotNull("No 'compactionCounter' metric found.", compactionCounter);
	long oldNumOfCompations = compactionCounter.getCount();

	ExecutorService fixedThreadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
	Map<Integer, Future<Void>> futures = new HashMap<>();
	for (int threadId = 0; threadId < THREAD_POOL_SIZE; threadId++) {
	    final int id = threadId;
	    Callable<Void> callable = new Callable<Void>() {
		@Override
		public Void call() throws Exception {
		    final int offset = id * NUMBER_OF_VALUES;
		    for (int valueId = 0; valueId < NUMBER_OF_VALUES; valueId++) {
			Key rowKey = Key.of(offset);
			byte[] value = Bytes.fromInt(offset + valueId);
			// Not presented, yet.
			byte[] readValue = store.get(rowKey);
			assertNull(readValue);
			// Put value.
			store.put(rowKey, value);
			// Check value.
			readValue = store.get(rowKey);
			assertNotNull(readValue);
			assertEquals(0, ByteArrayComparator.compareArrays(value, readValue));
			// Delete row.
			store.delete(rowKey);
			// Check deletion.
			readValue = store.get(rowKey);
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
	for (Future<Void> future : futures.values()) {
	    future.get();
	}
	assertTrue("No compactions were run, but this is the intention of the test.",
		oldNumOfCompations < compactionCounter.getCount());
	Timer time = (Timer) store.getMetric(LogStructuredStoreMetric.COMPACTION_TIMER);
	time.getSnapshot().dump(System.out);
    }

    @Test
    public void testSimpleConcurrencyCRUDonUniqueKeys() throws InterruptedException, ExecutionException {
	Counter compactionCounter = (Counter) store.getMetric(LogStructuredStoreMetric.COMPACTION_COUNTER);
	assertNotNull("No 'compactionCounter' metric found.", compactionCounter);
	long oldNumOfCompations = compactionCounter.getCount();

	ExecutorService fixedThreadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
	Map<Integer, Future<Void>> futures = new HashMap<>();
	for (int threadId = 0; threadId < THREAD_POOL_SIZE; threadId++) {
	    final int id = threadId;
	    Callable<Void> callable = new Callable<Void>() {
		@Override
		public Void call() throws Exception {
		    final int offset = id * NUMBER_OF_VALUES;
		    for (int valueId = 0; valueId < NUMBER_OF_VALUES; valueId++) {
			Key rowKey = Key.of(offset + valueId);
			byte[] value = Bytes.fromInt(offset + valueId);

			// Not presented, yet.
			byte[] readValue = store.get(rowKey);
			assertNull(readValue);
			// Put value.
			store.put(rowKey, value);
			// Check value.
			readValue = store.get(rowKey);
			assertNotNull(readValue);
			assertEquals(0, ByteArrayComparator.compareArrays(value, readValue));
			// Delete row.
			store.delete(rowKey);
			// Check deletion.
			readValue = store.get(rowKey);
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
	for (Future<Void> future : futures.values()) {
	    future.get();
	}
	assertTrue("No compactions were run, but this is the intention of the test.",
		oldNumOfCompations < compactionCounter.getCount());
	Timer time = (Timer) store.getMetric(LogStructuredStoreMetric.COMPACTION_TIMER);
	time.getSnapshot().dump(System.out);
    }

}

package com.puresoltechnologies.ductiledb.logstore;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;

public class LogStructuredStorePerformanceIT extends AbstractLogStructuredStoreTest {

    private static final int THREAD_POOL_SIZE = 64;
    private static final int NUMBER_OF_VALUES = 1000;

    private final LogStructuredStore store = getStore();

    @Test
    public void test() throws InterruptedException, ExecutionException {
	Counter compactionCounter = (Counter) store.getMetric(LogStructuredStoreMetric.COMPACTION_COUNTER);
	assertNotNull("No 'compactionCounter' metric found.", compactionCounter);
	long oldNumOfCompations = compactionCounter.getCount();

	LogStructuredStoreTestUtils.fillStore(store, new Supplier<Key>() {
	    private long i = 0;

	    @Override
	    public Key get() {
		if (i < 1_000_000) {
		    i++;
		    return Key.of(i);
		}
		return null;
	    }
	}, new Function<Key, byte[]>() {
	    @Override
	    public byte[] apply(Key key) {
		return key.getBytes();
	    }
	});

	assertTrue("No compactions were run, but this is the intention of the test.",
		oldNumOfCompations < compactionCounter.getCount());
	Timer time = (Timer) store.getMetric(LogStructuredStoreMetric.COMPACTION_TIMER);
	time.getSnapshot().dump(System.out);
    }
}

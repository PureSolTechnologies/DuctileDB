package com.puresoltechnologies.ductiledb.logstore;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.logstore.index.IndexEntry;
import com.puresoltechnologies.ductiledb.logstore.index.IndexFileReader;
import com.puresoltechnologies.ductiledb.logstore.index.IndexFileWriter;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class IndexFileConcurrencyIT extends AbstractLogStructuredStoreTest {

    private static final int THREAD_POOL_SIZE = 64;
    private static final int NUMBER_OF_VALUES = 1000;

    private final Storage storage = getStorage();

    @Test
    public void test() throws IOException, InterruptedException, ExecutionException {
	try (IndexFileWriter writer = new IndexFileWriter(storage, new File("IndexFileConcurrencyIT.index"),
		new File("IndexFileConcurrencyIT.data"))) {
	    ExecutorService fixedThreadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
	    Map<Integer, Future<Void>> futures = new HashMap<>();
	    for (int threadId = 0; threadId < THREAD_POOL_SIZE; threadId++) {
		final int id = threadId;
		Callable<Void> callable = new Callable<Void>() {
		    @Override
		    public Void call() throws Exception {
			long offset = id * NUMBER_OF_VALUES;
			Key rowKey = Key.of(offset);
			for (int valueId = 0; valueId < NUMBER_OF_VALUES; valueId++) {
			    writer.writeIndexEntry(rowKey, offset);
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
	}
	for (int threadId = 0; threadId < THREAD_POOL_SIZE; threadId++) {
	    final int id = threadId;
	    final int offset = id * NUMBER_OF_VALUES;
	    try (IndexFileReader reader = new IndexFileReader(storage, new File("IndexFileConcurrencyIT.index"))) {
		Key rowKey = Key.of(offset);
		for (int valueId = 0; valueId < NUMBER_OF_VALUES; valueId++) {
		    IndexEntry indexEntry = reader.get(rowKey);
		    assertEquals(rowKey, indexEntry.getRowKey());
		    assertEquals(offset, indexEntry.getOffset());
		}
	    }
	}

    }

}

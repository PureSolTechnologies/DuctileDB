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

import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.logstore.data.DataFileReader;
import com.puresoltechnologies.ductiledb.logstore.data.DataFileWriter;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class DataFileConcurrencyIT extends AbstractLogStructuredStoreTest {

    private static final int THREAD_POOL_SIZE = 64;
    private static final int NUMBER_OF_VALUES = 1000;

    private final Storage storage = getStorage();

    @Test
    public void test() throws IOException, InterruptedException, ExecutionException {
	try (DataFileWriter writer = new DataFileWriter(storage, new File("DataFileConcurrencyIT.data"))) {
	    ExecutorService fixedThreadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
	    Map<Integer, Future<Void>> futures = new HashMap<>();
	    for (int threadId = 0; threadId < THREAD_POOL_SIZE; threadId++) {
		final int id = threadId;
		Callable<Void> callable = new Callable<Void>() {
		    @Override
		    public Void call() throws Exception {
			int offset = id * NUMBER_OF_VALUES;
			Key rowKey = Key.of(offset);
			for (int valueId = 0; valueId < NUMBER_OF_VALUES; valueId++) {
			    byte[] value = Bytes.fromInt(offset + valueId);
			    writer.writeRow(rowKey, null, value);
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
	    try (DataFileReader reader = new DataFileReader(storage, new File("DataFileConcurrencyIT.data"))) {
		Key rowKey = Key.of(offset);
		for (int valueId = 0; valueId < NUMBER_OF_VALUES; valueId++) {
		    Row row = reader.readRow(rowKey);
		    assertEquals(rowKey, row.getKey());
		    assertEquals(offset + valueId, Bytes.toInt(row.getData()));
		}
	    }
	}

    }

}

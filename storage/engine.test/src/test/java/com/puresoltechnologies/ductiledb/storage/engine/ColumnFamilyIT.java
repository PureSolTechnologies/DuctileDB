package com.puresoltechnologies.ductiledb.storage.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.storage.engine.utils.Bytes;
import com.puresoltechnologies.ductiledb.storage.spi.FileStatus;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class ColumnFamilyIT extends AbstractStorageEngineTest {

    @Test
    public void testSmallDataAmount() throws IOException {
	DatabaseEngine engine = getEngine();

	byte[] rowKey1 = Bytes.toBytes(1l);
	byte[] rowKey2 = Bytes.toBytes(2l);
	byte[] rowKey3 = Bytes.toBytes(3l);

	Map<byte[], byte[]> values1 = new HashMap<>();
	values1.put(Bytes.toBytes(11l), Bytes.toBytes(111l));
	values1.put(Bytes.toBytes(12l), Bytes.toBytes(112l));
	values1.put(Bytes.toBytes(13l), Bytes.toBytes(113l));

	Map<byte[], byte[]> values2 = new HashMap<>();
	values2.put(Bytes.toBytes(21l), Bytes.toBytes(211l));
	values2.put(Bytes.toBytes(22l), Bytes.toBytes(212l));

	Map<byte[], byte[]> values3 = new HashMap<>();
	values3.put(Bytes.toBytes(31l), Bytes.toBytes(311l));

	byte[] timestamp = Bytes.toBytes(Instant.now());
	try (ColumnFamily bucket = new ColumnFamily(engine.getStorage(), new File("bucketIT"), getConfiguration())) {
	    bucket.put(timestamp, rowKey1, values1);
	    bucket.put(timestamp, rowKey2, values2);
	    bucket.put(timestamp, rowKey3, values3);

	    Map<byte[], byte[]> returned1 = bucket.get(rowKey1);
	    assertEquals(3, returned1.size());
	    assertEquals(111l, Bytes.toLong(returned1.get(Bytes.toBytes(11l))));
	    assertEquals(112l, Bytes.toLong(returned1.get(Bytes.toBytes(12l))));
	    assertEquals(113l, Bytes.toLong(returned1.get(Bytes.toBytes(13l))));
	    Map<byte[], byte[]> returned2 = bucket.get(rowKey2);
	    assertEquals(2, returned2.size());
	    assertEquals(211l, Bytes.toLong(returned2.get(Bytes.toBytes(21l))));
	    assertEquals(212l, Bytes.toLong(returned2.get(Bytes.toBytes(22l))));
	    Map<byte[], byte[]> returned3 = bucket.get(rowKey3);
	    assertEquals(1, returned3.size());
	    assertEquals(311l, Bytes.toLong(returned3.get(Bytes.toBytes(31l))));
	}
    }

    @Test
    public void testSSTableCreation() throws IOException {
	DatabaseEngine engine = getEngine();
	Storage storage = engine.getStorage();
	File bucketIDDirectory = new File("bucketIT");
	if (storage.exists(bucketIDDirectory)) {
	    storage.removeDirectory(bucketIDDirectory, true);
	}
	File commitLogFile = new File("bucketIT/commit.log");
	try (ColumnFamily bucket = new ColumnFamily(storage, bucketIDDirectory, getConfiguration())) {
	    Instant start = Instant.now();
	    byte[] timestamp = Bytes.toBytes(Instant.now());
	    bucket.setMaxCommitLogSize(1024 * 1024);
	    long commitLogSize = 0;
	    long lastCommitLogSize = 0;
	    long rowKey = 0;
	    while (lastCommitLogSize <= commitLogSize) {
		lastCommitLogSize = commitLogSize;
		rowKey++;
		Map<byte[], byte[]> values = new HashMap<>();
		for (long i = 1; i <= 10; i++) {
		    byte[] value = Bytes.toBytes(rowKey * i);
		    values.put(value, value);
		}
		bucket.put(timestamp, Bytes.toBytes(rowKey), values);
		FileStatus fileStatus = storage.getFileStatus(commitLogFile);
		commitLogSize = fileStatus.getLength();
		Instant stop = Instant.now();
		Duration duration = Duration.between(start, stop);
		System.out.println("count: " + rowKey + "; size: " + commitLogSize + "; t=" + duration.toMillis()
			+ "ms; perf=" + commitLogSize / 1024.0 / duration.toMillis() * 1000.0 + "kB/ms");
	    }
	    Iterator<File> bucketFiles = storage.list(bucketIDDirectory);
	    File sstableFile = null;
	    while (bucketFiles.hasNext()) {
		File bucketFile = bucketFiles.next();
		if (bucketFile.getName().endsWith(".sstable")) {
		    if (sstableFile == null) {
			sstableFile = bucketFile;
		    } else {
			fail("Only on sstable file is expected.");
		    }
		}
	    }
	    assertNotNull(sstableFile);
	}
    }

}

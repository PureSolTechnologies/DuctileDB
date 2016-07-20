package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.io.CommitLogWriter;
import com.puresoltechnologies.ductiledb.storage.engine.io.CountingOutputStream;
import com.puresoltechnologies.ductiledb.storage.engine.io.SSTableWriter;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.Memtable;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.MemtableFactory;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.RowMap;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.utils.StopWatch;
import com.puresoltechnologies.ductiledb.storage.spi.FileStatus;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class handles the storage of a single column family.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class ColumnFamilyEngine implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyEngine.class);

    private final ExecutorService compactionExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {

	@Override
	public Thread newThread(Runnable r) {
	    return new Thread(r, "ductiledb-compaction");
	}
    });

    private long maxCommitLogSize;

    private final Memtable memtable;
    private final Storage storage;
    private final ColumnFamilyDescriptor columnFamilyDescriptor;
    private final File commitLogFile;
    private CommitLogWriter commitLogWriter;
    private CountingOutputStream commitLogSizeStream;
    private final int bufferSize;

    public ColumnFamilyEngine(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor,
	    DatabaseEngineConfiguration configuration) throws StorageException {
	super();
	logger.info("Starting column family engine '" + columnFamilyDescriptor.getName() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	this.storage = storage;
	this.columnFamilyDescriptor = columnFamilyDescriptor;
	this.commitLogFile = new File(columnFamilyDescriptor.getDirectory(), "commit.log");
	this.memtable = MemtableFactory.create();
	this.maxCommitLogSize = configuration.getMaxCommitLogSize();
	this.bufferSize = configuration.getBufferSize();
	open();
	stopWatch.stop();
	logger.info("Column family engine '" + columnFamilyDescriptor.getName() + "' started in "
		+ stopWatch.getMillis() + "ms.");
    }

    private void open() throws StorageException {
	try {
	    if (!storage.exists(columnFamilyDescriptor.getDirectory())) {
		storage.createDirectory(columnFamilyDescriptor.getDirectory());
	    }
	    if (storage.exists(commitLogFile)) {
		FileStatus fileStatus = storage.getFileStatus(commitLogFile);
		commitLogSizeStream = new CountingOutputStream(new BufferedOutputStream(storage.append(commitLogFile)),
			fileStatus.getLength());
		commitLogWriter = new CommitLogWriter(commitLogSizeStream);
	    } else {
		createEmptyCommitLog();
	    }
	} catch (IOException e) {
	    throw new StorageException("Could not initialize column family '" + columnFamilyDescriptor.getName() + "'.",
		    e);
	}
    }

    @Override
    public void close() {
	logger.info("Closing column family engine '" + columnFamilyDescriptor.getName() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	if (commitLogWriter != null) {
	    try {
		commitLogWriter.close();
	    } catch (IOException e) {
		logger.warn("Could not cleanly close commit log.", e);
	    }
	}
	compactionExecutor.shutdown();
	try {
	    compactionExecutor.awaitTermination(1, TimeUnit.MINUTES);
	} catch (InterruptedException e) {
	    logger.warn("Shutdown of sstable creation executor took too long.", e);
	}
	stopWatch.stop();
	logger.info("Column family engine '" + columnFamilyDescriptor.getName() + "' closed in " + stopWatch.getMillis()
		+ "ms.");
    }

    public synchronized void put(byte[] timestamp, byte[] rowKey, Map<byte[], byte[]> values) throws StorageException {
	writeCommitLog(rowKey, values);
	writeMemtable(rowKey, values);
	if (commitLogSizeStream.getCount() > maxCommitLogSize) {
	    rolloverCommitLog();
	}
    }

    private void writeCommitLog(byte[] rowKey, Map<byte[], byte[]> values) throws StorageException {
	try {
	    for (Entry<byte[], byte[]> value : values.entrySet()) {
		commitLogWriter.write(rowKey, value.getKey(), value.getValue());
	    }
	} catch (IOException e) {
	    throw new StorageException("Could nto write commit.log.", e);
	}
    }

    private void writeMemtable(byte[] rowKey, Map<byte[], byte[]> values) {
	for (Entry<byte[], byte[]> value : values.entrySet()) {
	    memtable.put(rowKey, value.getKey(), value.getValue());
	}
    }

    private void rolloverCommitLog() throws StorageException {
	try {
	    logger.info("Max commit.log size of " + maxCommitLogSize + "bytes reached.");
	    Instant timestamp = Instant.now();
	    String baseFilename = String.valueOf(timestamp.getEpochSecond()) + "-"
		    + String.valueOf(timestamp.getNano());
	    createNewSSTableSegment(baseFilename);
	    createEmptyCommitLog();
	    memtable.clear();
	    compactionExecutor.submit(new Runnable() {
		@Override
		public void run() {
		    compaction();
		}
	    });
	} catch (IOException e) {
	    throw new StorageException("Could not rollover commit.log", e);
	}
    }

    private void createNewSSTableSegment(String baseFilename) throws IOException {
	logger.info("Creating new SSTtable segment...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	try (SSTableWriter ssTableWriter = new SSTableWriter(storage, columnFamilyDescriptor.getDirectory(),
		baseFilename, bufferSize)) {
	    RowMap values = memtable.getValues();
	    for (Entry<byte[], ColumnMap> row : values.entrySet()) {
		ssTableWriter.write(row.getKey(), row.getValue());
	    }
	}
	stopWatch.stop();
	logger.info("New SSTtable segment created in " + stopWatch.getMillis() + "ms.");
    }

    private void createEmptyCommitLog() throws StorageException {
	try {
	    if (commitLogWriter != null) {
		commitLogWriter.close();
		commitLogWriter = null;
		if (commitLogSizeStream != null) {
		    commitLogSizeStream.close();
		    commitLogSizeStream = null;
		}
		if (!storage.delete(commitLogFile)) {
		    throw new IOException("Could not delete commit.log file.");
		}
	    }
	    commitLogSizeStream = new CountingOutputStream(storage.create(commitLogFile));
	    commitLogWriter = new CommitLogWriter(commitLogSizeStream);
	} catch (IOException e) {
	    throw new StorageException("Could not create empty commit.log.", e);
	}
    }

    private void compaction() {

    }

    public Map<byte[], byte[]> get(byte[] rowKey) {
	return memtable.get(rowKey);
    }

    public void setMaxCommitLogSize(int maxCommitLogSize) {
	this.maxCommitLogSize = maxCommitLogSize;
    }
}

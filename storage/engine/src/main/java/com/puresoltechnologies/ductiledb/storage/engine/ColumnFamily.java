package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.engine.io.CommitLogWriter;
import com.puresoltechnologies.ductiledb.storage.engine.io.CountingOutputStream;
import com.puresoltechnologies.ductiledb.storage.engine.io.SSTableWriter;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.Memtable;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.MemtableFactory;
import com.puresoltechnologies.ductiledb.storage.engine.utils.StopWatch;
import com.puresoltechnologies.ductiledb.storage.spi.FileStatus;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class handles the storage of a single column family.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class ColumnFamily implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ColumnFamily.class);

    private static final long DEFAULT_MAX_COMMIT_LOG_SIZE = 1024 * 1024; // 1MB

    private long maxCommitLogSize = DEFAULT_MAX_COMMIT_LOG_SIZE;

    private final Memtable memtable;
    private final Storage storage;
    private final DatabaseEngineConfiguration configuration;
    private final File directory;
    private final File commitLogFile;
    private CommitLogWriter commitLogWriter;
    private CountingOutputStream commitLogSizeStream;

    public ColumnFamily(Storage storage, File directory, DatabaseEngineConfiguration configuration) throws IOException {
	super();
	logger.info("Starting database engine '" + directory.getName() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	this.storage = storage;
	this.directory = directory;
	this.commitLogFile = new File(directory, "commit.log");
	this.memtable = MemtableFactory.create();
	this.configuration = configuration;
	setMaxCommitLogSize(configuration.getMaxCommitLogSize());
	if (!storage.exists(directory)) {
	    storage.createDirectory(directory);
	}
	open();
	stopWatch.stop();
	logger.info("Database engine '" + directory.getName() + "' started in " + stopWatch.getMillis() + "ms.");
    }

    private void open() throws IOException {
	if (storage.exists(commitLogFile)) {
	    FileStatus fileStatus = storage.getFileStatus(commitLogFile);
	    commitLogSizeStream = new CountingOutputStream(storage.append(commitLogFile), fileStatus.getLength());
	    commitLogWriter = new CommitLogWriter(commitLogSizeStream);
	} else {
	    createEmptyCommitLog();
	}
    }

    @Override
    public void close() throws IOException {
	logger.info("Closing database engine '" + directory.getName() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	if (commitLogWriter != null) {
	    commitLogWriter.close();
	}
	stopWatch.stop();
	logger.info("Database engine '" + directory.getName() + "' closed in " + stopWatch.getMillis() + "ms.");
    }

    public void setMaxCommitLogSize(long maxSize) {
	this.maxCommitLogSize = maxSize;
    }

    public synchronized void put(byte[] timestamp, byte[] rowKey, Map<byte[], byte[]> values) throws IOException {
	for (Entry<byte[], byte[]> value : values.entrySet()) {
	    commitLogWriter.write(rowKey, value.getKey(), value.getValue());
	}
	for (Entry<byte[], byte[]> value : values.entrySet()) {
	    memtable.put(rowKey, value.getKey(), value.getValue());
	}
	if (commitLogSizeStream.getCount() > maxCommitLogSize) {
	    logger.info("Max commit.log size of " + maxCommitLogSize + "bytes reached.");
	    createSSTableSegment();
	    createEmptyCommitLog();
	    memtable.clear();
	}
    }

    private void createSSTableSegment() throws IOException {
	logger.info("Creating new SSTtable segment...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	Instant timestamp = Instant.now();
	File sstableFile = new File(directory,
		String.valueOf(timestamp.getEpochSecond()) + "-" + String.valueOf(timestamp.getNano()) + ".sstable");
	File indexFile = new File(directory,
		String.valueOf(timestamp.getEpochSecond()) + "-" + String.valueOf(timestamp.getNano()) + ".index");
	try (SSTableWriter ssTableWriter = new SSTableWriter(storage.create(sstableFile),
		configuration.getStorage().getBlockSize())) {
	    Map<byte[], Map<byte[], byte[]>> values = memtable.getValues();
	    for (Entry<byte[], Map<byte[], byte[]>> row : values.entrySet()) {
		ssTableWriter.write(row.getKey(), row.getValue());
	    }
	    ssTableWriter.close();
	}
	storage.create(indexFile).close();
	stopWatch.stop();
	logger.info("New SSTtable segment created in " + stopWatch.getMillis() + "ms.");
    }

    private void createEmptyCommitLog() throws IOException {
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
    }

    public Map<byte[], byte[]> get(byte[] rowKey) {
	return memtable.get(rowKey);
    }
}

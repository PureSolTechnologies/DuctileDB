package com.puresoltechnologies.ductiledb.logstore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.logstore.data.DataFileReader;
import com.puresoltechnologies.ductiledb.logstore.data.DataFileSet;
import com.puresoltechnologies.ductiledb.logstore.data.DataFileWriter;
import com.puresoltechnologies.ductiledb.logstore.index.IndexEntry;
import com.puresoltechnologies.ductiledb.logstore.index.IndexFileReader;
import com.puresoltechnologies.ductiledb.logstore.index.IndexFileWriter;
import com.puresoltechnologies.ductiledb.logstore.index.Memtable;
import com.puresoltechnologies.ductiledb.logstore.io.filter.CommitLogFilenameFilter;
import com.puresoltechnologies.ductiledb.logstore.io.filter.MetadataFilenameFilter;
import com.puresoltechnologies.ductiledb.logstore.io.filter.UsableCommitLogFilenameFilter;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageOutputStream;

/**
 * This is the actual implementation of a {@link LogStructuredStore}.
 * 
 * @author Rick-Rainer Ludwig
 */
public class LogStructuredStoreImpl implements LogStructuredStore {

    private static final Logger logger = LoggerFactory.getLogger(LogStructuredStoreImpl.class);

    /**
     * This executor is used to run scheduled compactions as a single thread.
     */
    private final ExecutorService compactionExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
	@Override
	public Thread newThread(Runnable r) {
	    return new Thread(r, "ductiledb-compaction");
	}
    });

    private final ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(true);
    private final ReadLock readLock = reentrantReadWriteLock.readLock();
    private final WriteLock writeLock = reentrantReadWriteLock.writeLock();

    private final Memtable memtable = new Memtable();
    private File commitLogFile = null;
    private DataFileWriter commitLogWriter = null;
    private DataFileSet dataSet = null;
    private boolean runCompactions = true;

    private final Storage storage;
    private final File directory;
    private final LogStoreConfiguration configuration;

    /**
     * This field contains the {@link MetricRegistry} for the metrics of the store.
     */
    private final MetricRegistry registry = new MetricRegistry();
    private final Counter compactionCounter;
    private final Timer compactionTime;
    private final Timer sstableGenerationTimer;

    LogStructuredStoreImpl(//
	    Storage storage, //
	    File directory, //
	    LogStoreConfiguration configuration) {
	super();
	this.storage = storage;
	this.directory = directory;
	this.configuration = configuration;
	compactionCounter = registry.counter(LogStructuredStoreMetric.COMPACTION_COUNTER.name());
	compactionTime = registry.timer(LogStructuredStoreMetric.COMPACTION_TIMER.name());
	sstableGenerationTimer = registry.timer(LogStructuredStoreMetric.SSTABLE_GENERATION_TIMER.name());
    }

    public final Storage getStorage() {
	return storage;
    }

    public final File getDirectory() {
	return directory;
    }

    public LogStoreConfiguration getConfiguration() {
	return configuration;
    }

    public final long getMaxCommitLogSize() {
	return configuration.getMaxCommitLogSize();
    }

    public final void setMaxCommitLogSize(long maxCommitLogSize) {
	configuration.setMaxCommitLogSize(maxCommitLogSize);
    }

    public final long getMaxDataFileSize() {
	return configuration.getMaxDataFileSize();
    }

    public final void setMaxDataFileSize(long maxDataFileSize) {
	configuration.setMaxDataFileSize(maxDataFileSize);
    }

    public final int getBufferSize() {
	return configuration.getBufferSize();
    }

    public final int getMaxFileGenerations() {
	return configuration.getMaxFileGenerations();
    }

    public final boolean isRunCompactions() {
	return runCompactions;
    }

    public final void setRunCompactions(boolean runCompactions) {
	this.runCompactions = runCompactions;
    }

    public final ReadLock getReadLock() {
	return readLock;
    }

    public final WriteLock getWriteLock() {
	return writeLock;
    }

    @Override
    public String toString() {
	return "LogStructuredStore:" + directory.getPath();
    }

    @Override
    public void close() {
	logger.info("Closing column family engine '" + toString() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	if (commitLogWriter != null) {
	    try {
		commitLogWriter.close();
		commitLogWriter = null;
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
	logger.info("Column family engine '" + toString() + "' closed in " + stopWatch.getMillis() + "ms.");
    }

    @Override
    public void open() {
	try {
	    logger.info("Starting column family engine '" + toString() + "'...");
	    StopWatch stopWatch = new StopWatch();
	    stopWatch.start();
	    if (!storage.exists(directory)) {
		storage.createDirectory(directory);
	    }
	    processExistingCommitLog();
	    createEmptyCommitLog();
	    checkDataFiles();
	    openDataFiles();
	    stopWatch.stop();
	    logger.info("Column family engine '" + toString() + "' started in " + stopWatch.getMillis() + "ms.");
	} catch (IOException e) {
	    throw new StorageException("Could not initialize column family '" + toString() + "'.", e);
	}
    }

    private void processExistingCommitLog() throws IOException, StorageException {
	for (File commitLog : storage.list(directory, new CommitLogFilenameFilter())) {
	    File indexName = DataFileSet.getIndexName(commitLog);
	    if (!storage.exists(indexName)) {
		createIndex(commitLog, indexName);
	    }
	    runCompaction(commitLog);
	}
    }

    private void createIndex(File commitLog, File indexName) {
	try (DataFileReader dataFileReader = new DataFileReader(storage, commitLog)) {
	    long offset = dataFileReader.getPosition();
	    Row row = dataFileReader.readRow();
	    Memtable memtable = new Memtable();
	    while (row != null) {
		if (row.getData() != null) {
		    memtable.put(new IndexEntry(row.getKey(), indexName, offset));
		}
		offset = dataFileReader.getPosition();
		row = dataFileReader.readRow();
	    }
	    try (IndexFileWriter indexFileWriter = new IndexFileWriter(storage, indexName, commitLog)) {
		for (IndexEntry entry : memtable) {
		    indexFileWriter.writeIndexEntry(entry.getRowKey(), entry.getOffset());
		}
	    }
	} catch (IOException e) {
	    throw new StorageException("Could not create index for commit log '" + commitLog + "'.", e);
	}
    }

    private void createEmptyCommitLog() {
	try {
	    if (commitLogWriter != null) {
		commitLogWriter.close();
		commitLogWriter = null;
	    }
	    memtable.clear();
	    commitLogFile = new File(directory,
		    LogStructuredStore.createBaseFilename(COMMIT_LOG_PREFIX) + DATA_FILE_SUFFIX);
	    commitLogWriter = new DataFileWriter(storage, commitLogFile);
	} catch (IOException e) {
	    throw new StorageException("Could not create empty " + commitLogFile.getName() + ".", e);
	}
    }

    private void checkDataFiles() {
	for (File file : storage.list(directory, new MetadataFilenameFilter())) {
	    new DataFileSet(storage, directory, file).check();
	    // TODO
	}
    }

    private void openDataFiles() {
	dataSet = new DataFileSet(storage, directory);
    }

    @Override
    public Metric getMetric(LogStructuredStoreMetric metric) {
	return registry.getMetrics().get(metric.name());
    }

    public void runCompaction() {
	for (File commitLog : getCurrentCommitLogs()) {
	    runCompaction(commitLog);
	}
	try {
	    rolloverCommitLog();
	} catch (StorageException e) {
	    logger.warn("Could not run compaction", e);
	}
    }

    private List<File> getCurrentCommitLogs() {
	List<File> commitLogs = new ArrayList<>();
	for (File commitLog : storage.list(directory, new UsableCommitLogFilenameFilter(storage))) {
	    if (commitLog.equals(commitLogFile)) {
		continue;
	    }
	    commitLogs.add(commitLog);
	}
	Collections.sort(commitLogs);
	Collections.reverse(commitLogs);
	return commitLogs;
    }

    private void runCompaction(File commitLogFile) {
	if (runCompactions) {
	    compactionExecutor.submit(new Runnable() {
		@Override
		public void run() {
		    try {
			Context time = compactionTime.time();
			Compactor.run(storage, directory, commitLogFile, configuration.getBufferSize(),
				configuration.getMaxDataFileSize(), configuration.getMaxFileGenerations());
			openDataFiles();
			deleteCommitLogFiles(commitLogFile);
			compactionCounter.inc();
			time.stop();
		    } catch (Exception e) {
			logger.error("Could not run compaction.", e);
		    }
		}
	    });
	}
    }

    private void deleteCommitLogFiles(File commitLogFile) throws IOException {
	try (StorageOutputStream compacted = storage.create(LogStructuredStore.getCompactedName(commitLogFile))) {
	    compacted.write(Bytes.fromInstant(Instant.now()));
	}
    }

    private void rolloverCommitLog() {
	try {
	    if (commitLogWriter == null) {
		return;
	    }
	    logger.info("Roll over " + commitLogFile.getName() + " with " + configuration.getMaxCommitLogSize()
		    + " bytes.");
	    if (commitLogWriter.getPosition() == 0) {
		logger.info("Do not roll over " + commitLogFile.getName() + " because size is 0 bytes.");
		return;
	    }
	    File commitLogFileSave = this.commitLogFile;
	    createIndexFile();
	    createEmptyCommitLog();
	    runCompaction(commitLogFileSave);
	} catch (IOException e) {
	    throw new StorageException("Could not rollover " + commitLogFile.getName(), e);
	}
    }

    private void createIndexFile() throws IOException, StorageException {
	logger.info("Creating new SSTtable index...");
	Context timer = sstableGenerationTimer.time();
	File indexFile = DataFileSet.getIndexName(commitLogFile);
	try (IndexFileWriter indexFileWriter = new IndexFileWriter(storage, indexFile, commitLogFile)) {
	    for (IndexEntry indexEntry : memtable) {
		indexFileWriter.writeIndexEntry(indexEntry.getRowKey(), indexEntry.getOffset());
	    }
	    indexFileWriter.flush();
	}
	long millis = TimeUnit.NANOSECONDS.toMillis(timer.stop());
	logger.info("New SSTtable index created in " + millis + "ms.");
    }

    @Override
    public void put(Key rowKey, byte[] data) {
	writeLock.lock();
	try {
	    writeCommitLog(rowKey, null, data);
	} finally {
	    writeLock.unlock();
	}
    }

    @Override
    public byte[] get(Key rowKey) {
	readLock.lock();
	try {
	    // Read from memtable
	    {
		Row row = readFromMemtable(rowKey);
		if (row != null) {
		    return row.wasDeleted() ? null : row.getData();
		}
	    }
	    // Read from commit logs
	    {
		Row row = readFromCommitLogs(rowKey);
		if (row != null) {
		    return row.wasDeleted() ? null : row.getData();
		}
	    }
	    // Read from data files
	    {
		Row row = readFromDataFiles(rowKey);
		return row != null ? row.getData() : null;
	    }
	} finally {
	    readLock.unlock();
	}
    }

    private Row readFromMemtable(Key rowKey) {
	IndexEntry indexEntry = memtable.get(rowKey);
	if (indexEntry == null) {
	    return null;
	}
	try (DataFileReader dataFileReader = new DataFileReader(storage, commitLogFile)) {
	    dataFileReader.seek(indexEntry.getOffset());
	    Row row = dataFileReader.readRow();
	    return row == null || row.wasDeleted() ? null : row;
	} catch (IOException e) {
	    throw new StorageException("Could not read data from current commit log '" + commitLogFile + "'.", e);
	}
    }

    private Row readFromDataFiles(Key rowKey) {
	try {
	    return dataSet.getRow(rowKey);
	} catch (IOException e) {
	    throw new StorageException("Could not read data.", e);
	}
    }

    private Row readFromCommitLogs(Key rowKey) {
	try {
	    List<File> commitLogs = getCurrentCommitLogs();
	    for (File commitLog : commitLogs) {
		IndexEntry indexEntry = null;
		try (IndexFileReader reader = new IndexFileReader(storage, DataFileSet.getIndexName(commitLog))) {
		    indexEntry = reader.get(rowKey);
		} catch (FileNotFoundException e) {
		    logger.warn("Could not find index file.", e);
		}
		if (indexEntry != null) {
		    try (DataFileReader reader = new DataFileReader(storage, commitLog)) {
			Row row = reader.readRow(indexEntry);
			if (row != null) {
			    return row;
			}
		    } catch (FileNotFoundException e) {
			logger.warn("Could not find commit log.", e);
		    }
		}
	    }
	    return null;
	} catch (IOException e) {
	    throw new StorageException("Could not read commit log.", e);
	}
    }

    @Override
    public RowScanner getScanner(Key startRowKey, Key endRowKey) {
	try {
	    Memtable memtableCopy = new Memtable();
	    List<File> currentCommitLogs;
	    DataFileSet dataFiles;
	    readLock.lock();
	    try {
		currentCommitLogs = getCurrentCommitLogs();
		dataFiles = new DataFileSet(storage, directory);
		memtable.forEach(entry -> memtableCopy.put(entry));
	    } finally {
		readLock.unlock();
	    }
	    return new RowScannerImpl(storage, memtableCopy, currentCommitLogs, dataFiles, startRowKey, endRowKey);
	} catch (IOException e) {
	    throw new StorageException("Could not create ColumnFamilyScanner.", e);
	}
    }

    @Override
    public void delete(Key rowKey) {
	writeLock.lock();
	try {
	    writeCommitLog(rowKey, Instant.now(), Bytes.empty());
	} finally {
	    writeLock.unlock();
	}
    }

    public void writeCommitLog(Key rowKey, Instant tombstone, byte[] values) {
	try {
	    long offset = commitLogWriter.getPosition();
	    commitLogWriter.writeRow(rowKey, tombstone, values);
	    commitLogWriter.flush();
	    memtable.put(new IndexEntry(rowKey, commitLogFile, offset));
	    if (commitLogWriter.getPosition() >= configuration.getMaxCommitLogSize()) {
		rolloverCommitLog();
	    }
	} catch (IOException e) {
	    throw new StorageException("Could not write " + commitLogFile.getName() + ".", e);
	}
    }

}

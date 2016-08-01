package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.PeekingIterator;
import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.io.CommitLogFilenameFilter;
import com.puresoltechnologies.ductiledb.storage.engine.io.MetadataFilenameFilter;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.DataInputStream;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.DataOutputStream;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.IndexOutputStream;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.SSTableReader;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.SSTableSet;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.Memtable;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class handles the storage of a single column family.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class ColumnFamilyEngineImpl implements ColumnFamilyEngine {

    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyEngineImpl.class);

    private final ExecutorService compactionExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
	@Override
	public Thread newThread(Runnable r) {
	    return new Thread(r, "ductiledb-compaction");
	}
    });

    private long maxCommitLogSize;
    private long maxDataFileSize;

    private final Memtable memtable = new Memtable();
    private final Storage storage;
    private final ColumnFamilyDescriptor columnFamilyDescriptor;
    private File commitLogFile = null;
    private DataOutputStream commitLogStream;
    private final int bufferSize;
    private final int maxGenerations;
    private SSTableSet dataSet = null;

    public ColumnFamilyEngineImpl(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor,
	    DatabaseEngineConfiguration configuration) throws StorageException {
	super();
	logger.info("Starting column family engine '" + columnFamilyDescriptor.getName() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	this.storage = storage;
	this.columnFamilyDescriptor = columnFamilyDescriptor;
	this.maxCommitLogSize = configuration.getMaxCommitLogSize();
	this.maxDataFileSize = configuration.getMaxDataFileSize();
	this.bufferSize = configuration.getBufferSize();
	this.maxGenerations = configuration.getMaxFileGenerations();
	open();
	stopWatch.stop();
	logger.info("Column family engine '" + columnFamilyDescriptor.getName() + "' started in "
		+ stopWatch.getMillis() + "ms.");
    }

    public void setMaxCommitLogSize(int maxCommitLogSize) {
	this.maxCommitLogSize = maxCommitLogSize;
    }

    public void setMaxDataFileSize(long maxDataFileSize) {
	this.maxDataFileSize = maxDataFileSize;
    }

    private void open() throws StorageException {
	try {
	    if (!storage.exists(columnFamilyDescriptor.getDirectory())) {
		storage.createDirectory(columnFamilyDescriptor.getDirectory());
	    }
	    processExistingCommitLog();
	    createEmptyCommitLog();
	    checkDataFiles();
	    dataSet = new SSTableSet(storage, columnFamilyDescriptor);
	} catch (IOException e) {
	    throw new StorageException("Could not initialize column family '" + columnFamilyDescriptor.getName() + "'.",
		    e);
	}
    }

    private void checkDataFiles() throws FileNotFoundException {
	for (File file : storage.list(columnFamilyDescriptor.getDirectory(), new MetadataFilenameFilter())) {
	    new SSTableSet(storage, columnFamilyDescriptor, file).check();
	}
    }

    private void processExistingCommitLog() throws IOException, StorageException {
	for (File commitLog : storage.list(columnFamilyDescriptor.getDirectory(), new CommitLogFilenameFilter())) {
	    File indexName = SSTableSet.getIndexName(commitLog);
	    if (!storage.exists(indexName)) {
		createIndex(commitLog, indexName);
	    }
	    runCompaction(commitLog);
	}
    }

    private void createIndex(File commitLog, File indexName) throws StorageException {
	try (DataInputStream dataStream = new DataInputStream(storage.open(commitLog))) {
	    long offset = dataStream.getOffset();
	    ColumnFamilyRow row = dataStream.readRow();
	    Memtable memtable = new Memtable();
	    while (row != null) {
		memtable.put(new IndexEntry(row.getRowKey(), indexName, offset));
		offset = dataStream.getOffset();
		row = dataStream.readRow();
	    }
	    try (IndexOutputStream indexStream = new IndexOutputStream(storage.create(indexName), bufferSize)) {
		for (IndexEntry entry : memtable) {
		    indexStream.writeIndexEntry(entry.getRowKey(), entry.getOffset());
		}
	    }
	} catch (IOException e) {
	    throw new StorageException("Could not create index for commit log '" + commitLog + "'.", e);
	}
    }

    @Override
    public void close() {
	logger.info("Closing column family engine '" + columnFamilyDescriptor.getName() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	if (commitLogStream != null) {
	    try {
		commitLogStream.close();
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

    @Override
    public synchronized void put(byte[] timestamp, byte[] rowKey, ColumnMap values) throws StorageException {
	ColumnMap columnMap = get(rowKey);
	if (columnMap != null) {
	    columnMap.putAll(values);
	    values = columnMap;
	}
	long offset = commitLogStream.getOffset();
	RowKey rowKey2 = new RowKey(rowKey);
	writeCommitLog(rowKey2, values);
	writeMemtable(rowKey2, offset);
	if (offset > maxCommitLogSize) {
	    rolloverCommitLog();
	}
    }

    private void writeCommitLog(RowKey rowKey, ColumnMap values) throws StorageException {
	try {
	    commitLogStream.writeRow(rowKey, values);
	    commitLogStream.flush();
	} catch (IOException e) {
	    throw new StorageException("Could not write " + commitLogFile.getName() + ".", e);
	}
    }

    private void writeMemtable(RowKey rowKey, long offset) {
	memtable.put(new IndexEntry(rowKey, null, offset));
    }

    private void rolloverCommitLog() throws StorageException {
	try {
	    logger.info("Max " + commitLogFile.getName() + " size of " + maxCommitLogSize + "bytes reached.");
	    File commitLogFileSave = this.commitLogFile;
	    createIndexFile();
	    createEmptyCommitLog();
	    runCompaction(commitLogFileSave);
	} catch (IOException e) {
	    throw new StorageException("Could not rollover " + commitLogFile.getName(), e);
	}
    }

    public static String createBaseFilename(String filePrefix) {
	Instant timestamp = Instant.now();
	StringBuffer buffer = new StringBuffer(filePrefix);
	buffer.append('-');
	buffer.append(timestamp.getEpochSecond());
	int millis = timestamp.getNano() / 1000000;
	if (millis < 100) {
	    if (millis < 10) {
		buffer.append("00");
	    } else {
		buffer.append('0');
	    }
	}
	buffer.append(millis);
	return buffer.toString();
    }

    private void createIndexFile() throws IOException, StorageException {
	logger.info("Creating new SSTtable index...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	File indexFile = SSTableSet.getIndexName(commitLogFile);
	try (IndexOutputStream indexStream = new IndexOutputStream(storage.create(indexFile), bufferSize)) {
	    for (IndexEntry indexEntry : memtable) {
		indexStream.writeIndexEntry(indexEntry.getRowKey(), indexEntry.getOffset());
	    }
	    indexStream.flush();
	}
	stopWatch.stop();
	logger.info("New SSTtable index created in " + stopWatch.getMillis() + "ms.");
    }

    private void createEmptyCommitLog() throws StorageException {
	try {
	    if (commitLogStream != null) {
		commitLogStream.close();
		commitLogStream = null;
	    }
	    memtable.clear();
	    commitLogFile = new File(columnFamilyDescriptor.getDirectory(),
		    createBaseFilename(COMMIT_LOG_PREFIX) + DATA_FILE_SUFFIX);
	    commitLogStream = new DataOutputStream(storage.create(commitLogFile), bufferSize);
	} catch (IOException e) {
	    throw new StorageException("Could not create empty " + commitLogFile.getName() + ".", e);
	}
    }

    private void runCompaction(File commitLogFile) {
	compactionExecutor.submit(new Runnable() {
	    @Override
	    public void run() {
		try {
		    Compactor compactor = new Compactor(storage, columnFamilyDescriptor, commitLogFile, bufferSize,
			    maxDataFileSize, maxGenerations);
		    compactor.runCompaction();
		    dataSet = new SSTableSet(storage, columnFamilyDescriptor);
		} catch (StorageException | FileNotFoundException e) {
		    logger.error("Could not run compaction.", e);
		}
	    }
	});
    }

    @Override
    public ColumnMap get(byte[] rowKey) throws StorageException {
	RowKey rowKey2 = new RowKey(rowKey);
	IndexEntry indexEntry = memtable.get(rowKey2);
	ColumnMap columnMap;
	if ((indexEntry != null) && (!indexEntry.wasDeleted())) {
	    long offset = indexEntry.getOffset();
	    try (DataInputStream dataInputStream = new DataInputStream(storage.open(commitLogFile))) {
		dataInputStream.skip(offset);
		ColumnFamilyRow row = dataInputStream.readRow();
		if (row != null) {
		    return row.getColumnMap();
		}
	    } catch (IOException e) {
		logger.warn("Could not read data from current commit log '" + commitLogFile + "'.", e);
	    }

	}
	columnMap = readFromCommitLogs(rowKey2);
	if (columnMap != null) {
	    return columnMap;
	}
	return readFromDataFiles(rowKey2);
    }

    private ColumnMap readFromDataFiles(RowKey rowKey) throws StorageException {
	try {
	    return dataSet.get(rowKey);
	} catch (IOException e) {
	    throw new StorageException("Could not read data.", e);
	}
    }

    private ColumnMap readFromCommitLogs(RowKey rowKey) throws StorageException {
	List<File> commitLogs = new ArrayList<>();
	for (File commitLog : storage.list(columnFamilyDescriptor.getDirectory(), new CommitLogFilenameFilter())) {
	    if (commitLog.equals(commitLogFile)) {
		continue;
	    }
	    commitLogs.add(commitLog);
	}
	Collections.sort(commitLogs);
	Collections.reverse(commitLogs);
	for (File commitLog : commitLogs) {
	    SSTableReader reader = new SSTableReader(storage, commitLog);
	    ColumnMap entry = reader.readColumnMap(rowKey);
	    if (entry != null) {
		return entry;
	    }
	}
	return null;
    }

    @Override
    public synchronized void delete(byte[] rowKey) throws StorageException {
	writeMemtable(new RowKey(rowKey), -1);
    }

    @Override
    public void delete(byte[] rowKey, Set<byte[]> columns) throws StorageException {
	ColumnMap columnMap = get(rowKey);
	if (columnMap != null) {
	    for (byte[] columnKey : columns) {
		columnMap.remove(columnKey);
	    }
	    if (columnMap.size() == 0) {
		delete(rowKey);
	    } else {
		long offset = commitLogStream.getOffset();
		RowKey rowKey2 = new RowKey(rowKey);
		writeCommitLog(rowKey2, columnMap);
		writeMemtable(rowKey2, offset);
		if (offset > maxCommitLogSize) {
		    rolloverCommitLog();
		}
	    }
	}
    }

    @Override
    public ColumnFamilyScanner getScanner(NavigableSet<byte[]> columns) {
	return new ColumnFamilyScanner(this, columns);
    }

    public PeekingIterator<IndexEntry> getMemtableIterator() {
	return memtable.iterator();
    }
}

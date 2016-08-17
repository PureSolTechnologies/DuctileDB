package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.index.Memtable;
import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.io.DataFileSet;
import com.puresoltechnologies.ductiledb.storage.engine.io.MetadataFilenameFilter;
import com.puresoltechnologies.ductiledb.storage.engine.io.UsableCommitLogFilenameFilter;
import com.puresoltechnologies.ductiledb.storage.engine.io.data.DataFileReader;
import com.puresoltechnologies.ductiledb.storage.engine.io.data.DataInputStream;
import com.puresoltechnologies.ductiledb.storage.engine.io.data.DataOutputStream;
import com.puresoltechnologies.ductiledb.storage.engine.io.index.IndexFileReader;
import com.puresoltechnologies.ductiledb.storage.engine.io.index.IndexOutputStream;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class handles the storage of a single column family.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class ColumnFamilyEngineImpl implements ColumnFamilyEngine {

    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyEngineImpl.class);

    public static String createFilename(String filePrefix, Instant timestamp, int number, String suffix) {
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
	buffer.append("-");
	buffer.append(number);
	buffer.append(suffix);
	return buffer.toString();
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

    public static String createFilename(String baseFilename, int number, String suffix) {
	StringBuffer buffer = new StringBuffer(baseFilename);
	buffer.append("-");
	buffer.append(number);
	buffer.append(suffix);
	return buffer.toString();
    }

    public static File getCompactedName(File dataFile) {
	return new File(dataFile.getParent(), dataFile.getName().replace(ColumnFamilyEngineImpl.DATA_FILE_SUFFIX,
		ColumnFamilyEngineImpl.COMPACTED_FILE_SUFFIX));
    }

    private final ExecutorService compactionExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
	@Override
	public Thread newThread(Runnable r) {
	    return new Thread(r, "ductiledb-compaction");
	}
    });

    private long maxCommitLogSize;
    private long maxDataFileSize;
    private boolean runCompactions = true;

    private final Memtable memtable = new Memtable();
    private final Storage storage;
    private final ColumnFamilyDescriptor columnFamilyDescriptor;
    private File commitLogFile = null;
    private DataOutputStream commitLogStream;
    private final int bufferSize;
    private final int maxGenerations;
    private DataFileSet dataSet = null;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
    private final Lock readLock = readWriteLock.readLock();
    private final Lock writeLock = readWriteLock.readLock();

    public ColumnFamilyEngineImpl(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor,
	    DatabaseEngineConfiguration configuration) throws StorageException {
	super();
	this.storage = storage;
	this.columnFamilyDescriptor = columnFamilyDescriptor;
	this.maxCommitLogSize = configuration.getMaxCommitLogSize();
	this.maxDataFileSize = configuration.getMaxDataFileSize();
	this.bufferSize = configuration.getBufferSize();
	this.maxGenerations = configuration.getMaxFileGenerations();
	logger.info("Starting column family engine '" + toString() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	open();
	stopWatch.stop();
	logger.info("Column family engine '" + toString() + "' started in " + stopWatch.getMillis() + "ms.");
    }

    @Override
    public byte[] getName() {
	return columnFamilyDescriptor.getName();
    }

    @Override
    public ColumnFamilyDescriptor getDescriptor() {
	return columnFamilyDescriptor;
    }

    @Override
    public String toString() {
	TableDescriptor table = columnFamilyDescriptor.getTable();
	return "engine:" + table.getNamespace().getName() + "." + table.getName() + "/"
		+ Bytes.toHumanReadableString(columnFamilyDescriptor.getName());
    }

    public void setMaxCommitLogSize(int maxCommitLogSize) {
	this.maxCommitLogSize = maxCommitLogSize;
    }

    public void setMaxDataFileSize(long maxDataFileSize) {
	this.maxDataFileSize = maxDataFileSize;
    }

    public boolean isRunCompactions() {
	return runCompactions;
    }

    public void setRunCompactions(boolean runCompactions) {
	this.runCompactions = runCompactions;
    }

    private void open() throws StorageException {
	try {
	    if (!storage.exists(columnFamilyDescriptor.getDirectory())) {
		storage.createDirectory(columnFamilyDescriptor.getDirectory());
	    }
	    processExistingCommitLog();
	    createEmptyCommitLog();
	    checkDataFiles();
	    openDataFiles();
	} catch (IOException e) {
	    throw new StorageException("Could not initialize column family '" + toString() + "'.", e);
	}
    }

    private void openDataFiles() throws StorageException {
	dataSet = new DataFileSet(storage, columnFamilyDescriptor);
    }

    private void deleteCommitLogFiles(File commitLogFile) throws IOException {
	try (BufferedOutputStream compacted = storage.create(getCompactedName(commitLogFile))) {
	    compacted.write(Bytes.toBytes(Instant.now()));
	}
    }

    private void checkDataFiles() throws StorageException {
	for (File file : storage.list(columnFamilyDescriptor.getDirectory(), new MetadataFilenameFilter())) {
	    new DataFileSet(storage, columnFamilyDescriptor, file).check();
	}
    }

    private void processExistingCommitLog() throws IOException, StorageException {
	for (File commitLog : storage.list(columnFamilyDescriptor.getDirectory(),
		new UsableCommitLogFilenameFilter(storage))) {
	    File indexName = DataFileSet.getIndexName(commitLog);
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
		if (!row.getColumnMap().isEmpty()) {
		    memtable.put(new IndexEntry(row.getRowKey(), indexName, offset));
		}
		offset = dataStream.getOffset();
		row = dataStream.readRow();
	    }
	    try (IndexOutputStream indexStream = new IndexOutputStream(storage.create(indexName), bufferSize,
		    commitLog)) {
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
	logger.info("Closing column family engine '" + toString() + "'...");
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
	logger.info("Column family engine '" + toString() + "' closed in " + stopWatch.getMillis() + "ms.");
    }

    @Override
    public void put(byte[] rowKey, ColumnMap values) throws StorageException {
	writeLock.lock();
	try {
	    ColumnMap columnMap = get(rowKey);
	    if (columnMap != null) {
		columnMap.putAll(values);
		values = columnMap;
	    }
	    writeNewColumns(new RowKey(rowKey), values);
	} finally {
	    writeLock.unlock();
	}
    }

    private void writeCommitLog(RowKey rowKey, ColumnMap values, long offset) throws StorageException {
	try {
	    commitLogStream.writeRow(rowKey, values);
	    commitLogStream.flush();
	    memtable.put(new IndexEntry(rowKey, commitLogFile, offset));
	    if (commitLogStream.getOffset() >= maxCommitLogSize) {
		rolloverCommitLog();
	    }
	} catch (IOException e) {
	    throw new StorageException("Could not write " + commitLogFile.getName() + ".", e);
	}
    }

    private void rolloverCommitLog() throws StorageException {
	try {
	    logger.info("Roll over " + commitLogFile.getName() + " with " + maxCommitLogSize + " bytes.");
	    if (commitLogStream.getOffset() == 0) {
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
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	File indexFile = DataFileSet.getIndexName(commitLogFile);
	try (IndexOutputStream indexStream = new IndexOutputStream(storage.create(indexFile), bufferSize,
		commitLogFile)) {
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

    public void runCompaction() {
	writeLock.lock();
	try {
	    for (File commitLog : getCurrentCommitLogs()) {
		runCompaction(commitLog);
	    }
	    rolloverCommitLog();
	} catch (StorageException | IOException e) {
	    logger.warn("Could not run compaction", e);
	} finally {
	    writeLock.unlock();
	}
    }

    private void runCompaction(File commitLogFile) throws StorageException, IOException {
	if (runCompactions) {
	    compactionExecutor.submit(new Runnable() {
		@Override
		public void run() {
		    try {
			Compactor compactor = new Compactor(storage, columnFamilyDescriptor, commitLogFile, bufferSize,
				maxDataFileSize, maxGenerations);
			compactor.runCompaction();
			openDataFiles();
			deleteCommitLogFiles(commitLogFile);
		    } catch (Exception e) {
			logger.error("Could not run compaction.", e);
		    }
		}
	    });
	}
    }

    @Override
    public ColumnMap get(byte[] rowKey) throws StorageException {
	readLock.lock();
	try {
	    RowKey rowKey2 = new RowKey(rowKey);
	    IndexEntry indexEntry = memtable.get(rowKey2);
	    ColumnFamilyRow row;
	    if (indexEntry != null) {
		if (indexEntry.wasDeleted()) {
		    return new ColumnMap();
		}
		long offset = indexEntry.getOffset();
		try (DataInputStream dataInputStream = new DataInputStream(storage.open(commitLogFile))) {
		    dataInputStream.skip(offset);
		    row = dataInputStream.readRow();
		    if (row != null) {
			return row.getColumnMap();
		    }
		} catch (IOException e) {
		    throw new StorageException("Could not read data from current commit log '" + commitLogFile + "'.",
			    e);
		}
	    }
	    row = readFromCommitLogs(rowKey2);
	    if (row != null) {
		return row.getColumnMap();
	    }
	    row = readFromDataFiles(rowKey2);
	    return row != null ? row.getColumnMap() : new ColumnMap();
	} finally {
	    readLock.unlock();
	}
    }

    private ColumnFamilyRow readFromDataFiles(RowKey rowKey) throws StorageException {
	try {
	    return dataSet.getRow(rowKey);
	} catch (IOException e) {
	    throw new StorageException("Could not read data.", e);
	}
    }

    private ColumnFamilyRow readFromCommitLogs(RowKey rowKey) throws StorageException {
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
			ColumnFamilyRow entry = reader.getRow(indexEntry);
			if (entry != null) {
			    return entry;
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

    private List<File> getCurrentCommitLogs() {
	List<File> commitLogs = new ArrayList<>();
	for (File commitLog : storage.list(columnFamilyDescriptor.getDirectory(),
		new UsableCommitLogFilenameFilter(storage))) {
	    if (commitLog.equals(commitLogFile)) {
		continue;
	    }
	    commitLogs.add(commitLog);
	}
	Collections.sort(commitLogs);
	Collections.reverse(commitLogs);
	return commitLogs;
    }

    @Override
    public void delete(byte[] rowKey) throws StorageException {
	writeLock.lock();
	try {
	    RowKey rowKey2 = new RowKey(rowKey);
	    writeCommitLog(rowKey2, new ColumnMap(), -1);
	} finally {
	    writeLock.unlock();
	}
    }

    @Override
    public void delete(byte[] rowKey, Set<byte[]> columns) throws StorageException {
	writeLock.lock();
	try {
	    ColumnMap columnMap = get(rowKey);
	    if (columnMap != null) {
		for (byte[] columnKey : columns) {
		    columnMap.remove(columnKey);
		}
		if (columnMap.size() == 0) {
		    delete(rowKey);
		} else {
		    writeNewColumns(new RowKey(rowKey), columnMap);
		}
	    }
	} finally {
	    writeLock.unlock();
	}
    }

    @Override
    public ColumnFamilyScanner getScanner(byte[] startRowKey, byte[] endRowKey) throws StorageException {
	readLock.lock();
	try {
	    Memtable memtableCopy = new Memtable();
	    memtable.forEach(entry -> memtableCopy.put(entry));
	    List<File> currentCommitLogs = getCurrentCommitLogs();
	    DataFileSet dataFiles = new DataFileSet(storage, columnFamilyDescriptor);
	    return new ColumnFamilyScanner(storage, memtableCopy, currentCommitLogs, dataFiles,
		    startRowKey != null ? new RowKey(startRowKey) : null,
		    endRowKey != null ? new RowKey(endRowKey) : null);
	} catch (IOException e) {
	    throw new StorageException("Could not create ColumnFamilyScanner.", e);
	} finally {
	    readLock.lock();
	}
    }

    @Override
    public long incrementColumnValue(byte[] rowKey, byte[] column, long incrementValue) throws StorageException {
	return incrementColumnValue(rowKey, column, 1l, incrementValue);
    }

    @Override
    public long incrementColumnValue(byte[] rowKey, byte[] column, long startValue, long incrementValue)
	    throws StorageException {
	writeLock.lock();
	try {
	    ColumnMap columnMap = get(rowKey);
	    long result = startValue;
	    if (columnMap != null) {
		byte[] oldValueBytes = columnMap.get(column);
		if (oldValueBytes != null) {
		    long oldValue = Bytes.toLong(oldValueBytes);
		    result = oldValue + incrementValue;
		}
		columnMap.put(column, Bytes.toBytes(result));
	    } else {
		columnMap = new ColumnMap();
		columnMap.put(column, Bytes.toBytes(result));
	    }
	    writeNewColumns(new RowKey(rowKey), columnMap);
	    return result;
	} finally {
	    writeLock.unlock();
	}
    }

    private void writeNewColumns(RowKey rowKey, ColumnMap columnMap) throws StorageException {
	long offset = commitLogStream.getOffset();
	writeCommitLog(rowKey, columnMap, offset);
    }

}

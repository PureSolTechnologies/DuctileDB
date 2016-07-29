package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.index.Index;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexFactory;
import com.puresoltechnologies.ductiledb.storage.engine.index.OffsetRange;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
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
    private final Index index;
    private final ColumnFamilyDescriptor columnFamilyDescriptor;
    private File commitLogFile = null;
    private DataOutputStream commitLogStream;
    private final int bufferSize;
    private final int maxGenerations;

    public ColumnFamilyEngineImpl(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor,
	    DatabaseEngineConfiguration configuration) throws StorageException {
	super();
	logger.info("Starting column family engine '" + columnFamilyDescriptor.getName() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	this.storage = storage;
	this.columnFamilyDescriptor = columnFamilyDescriptor;
	this.index = IndexFactory.create(storage, columnFamilyDescriptor);
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
	    index.update();
	} catch (IOException e) {
	    throw new StorageException("Could not initialize column family '" + columnFamilyDescriptor.getName() + "'.",
		    e);
	}
    }

    private void checkDataFiles() {
	for (File file : storage.list(columnFamilyDescriptor.getDirectory(), new MetadataFilenameFilter())) {
	    new SSTableSet(storage, file).check();
	}
    }

    private void processExistingCommitLog() throws IOException, StorageException {
	// TODO: read all Commit-log, check for index and create if needed. Run
	// // compaction.
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
		memtable.put(row.getRowKey(), offset);
		offset = dataStream.getOffset();
		row = dataStream.readRow();
	    }
	    try (IndexOutputStream indexStream = new IndexOutputStream(storage.create(indexName), bufferSize)) {
		for (Entry<byte[], Long> entry : memtable.getValues().entrySet()) {
		    indexStream.writeIndexEntry(entry.getKey(), entry.getValue());
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
	writeCommitLog(rowKey, values);
	writeMemtable(rowKey, offset);
	if (offset > maxCommitLogSize) {
	    rolloverCommitLog();
	}
    }

    private void writeCommitLog(byte[] rowKey, ColumnMap values) throws StorageException {
	try {
	    commitLogStream.writeRow(rowKey, values);
	    commitLogStream.flush();
	} catch (IOException e) {
	    throw new StorageException("Could not write " + commitLogFile.getName() + ".", e);
	}
    }

    private void writeMemtable(byte[] rowKey, long offset) {
	memtable.put(rowKey, offset);
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
	    TreeMap<byte[], Long> offsets = memtable.getValues();
	    for (Entry<byte[], Long> row : offsets.entrySet()) {
		indexStream.writeIndexEntry(row.getKey(), row.getValue());
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
		    index.update();
		} catch (StorageException e) {
		    logger.error("Could not run compaction.", e);
		}
	    }
	});
    }

    @Override
    public ColumnMap get(byte[] rowKey) throws StorageException {
	long offset = memtable.get(rowKey);
	ColumnMap columnMap;
	if (offset >= 0) {
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
	columnMap = readFromCommitLogs(rowKey);
	if (columnMap != null) {
	    return columnMap;
	}
	return readFromDataFiles(rowKey);
    }

    private ColumnMap readFromDataFiles(byte[] rowKey) throws StorageException {
	OffsetRange offsetRange = index.find(rowKey);
	if (offsetRange == null) {
	    return null;
	}
	IndexEntry startOffset = offsetRange.getStartOffset();
	IndexEntry endOffset = offsetRange.getEndOffset();
	if (!startOffset.getDataFile().equals(endOffset.getDataFile())) {
	    throw new IllegalStateException("File overlapping index range for key '"
		    + Bytes.toHumanReadableString(rowKey) + "':\n" + startOffset + "\n" + endOffset);
	}
	SSTableReader reader = new SSTableReader(storage, startOffset.getDataFile());
	return reader.readColumnMap(rowKey, startOffset, endOffset);
    }

    private ColumnMap readFromCommitLogs(byte[] rowKey) throws StorageException {
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
	writeMemtable(rowKey, -1);
    }
}

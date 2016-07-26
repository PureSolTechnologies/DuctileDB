package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Map.Entry;
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
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.ColumnFamilyRowIterable;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.DataInputStream;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.DataOutputStream;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.SSTableReader;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.SSTableSet;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.SSTableWriter;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.Memtable;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.RowMap;
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
    private final File commitLogFile;
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
	this.commitLogFile = new File(columnFamilyDescriptor.getDirectory(), COMMIT_LOG_NAME);
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

    private void open() throws StorageException {
	try {
	    if (!storage.exists(columnFamilyDescriptor.getDirectory())) {
		storage.createDirectory(columnFamilyDescriptor.getDirectory());
	    }
	    if (storage.exists(commitLogFile)) {
		openCommitLog();
	    } else {
		createEmptyCommitLog();
	    }
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

    private void openCommitLog() throws IOException {
	try (DataInputStream dataInputStream = new DataInputStream(storage.open(commitLogFile))) {
	    try (ColumnFamilyRowIterable iterable = new ColumnFamilyRowIterable(dataInputStream)) {
		for (ColumnFamilyRow row : iterable) {
		    memtable.put(row.getRowKey(), row.getColumnMap());
		}
	    }
	}
	commitLogStream = new DataOutputStream(storage.append(commitLogFile), bufferSize);
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
	writeCommitLog(rowKey, values);
	writeMemtable(rowKey, values);
	if (commitLogStream.getOffset() > maxCommitLogSize) {
	    rolloverCommitLog();
	}
    }

    private void writeCommitLog(byte[] rowKey, ColumnMap values) throws StorageException {
	try {
	    commitLogStream.writeRow(rowKey, values);
	} catch (IOException e) {
	    throw new StorageException("Could not write " + COMMIT_LOG_NAME + ".", e);
	}
    }

    private void writeMemtable(byte[] rowKey, ColumnMap columnMap) {
	memtable.put(rowKey, columnMap);
    }

    private void rolloverCommitLog() throws StorageException {
	try {
	    logger.info("Max " + COMMIT_LOG_NAME + " size of " + maxCommitLogSize + "bytes reached.");
	    String baseFilename = createBaseFilename("CommitLog");
	    File commitLogFile = createNewSSTableSegment(baseFilename);
	    createEmptyCommitLog();
	    memtable.clear();
	    compactionExecutor.submit(new Runnable() {
		@Override
		public void run() {
		    try {
			runCompaction(commitLogFile);
		    } catch (StorageException e) {
			logger.error("Could not run compaction.", e);
		    }
		}
	    });
	} catch (IOException e) {
	    throw new StorageException("Could not rollover " + COMMIT_LOG_NAME, e);
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

    private File createNewSSTableSegment(String baseFilename) throws IOException, StorageException {
	logger.info("Creating new SSTtable segment...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	File commitLogFile;
	try (SSTableWriter ssTableWriter = new SSTableWriter(storage, columnFamilyDescriptor.getDirectory(),
		baseFilename, bufferSize)) {
	    RowMap values = memtable.getValues();
	    for (Entry<byte[], ColumnMap> row : values.entrySet()) {
		ssTableWriter.write(row.getKey(), row.getValue());
	    }
	    commitLogFile = ssTableWriter.getDataFile();
	}
	stopWatch.stop();
	logger.info("New SSTtable segment created in " + stopWatch.getMillis() + "ms.");
	return commitLogFile;
    }

    private void createEmptyCommitLog() throws StorageException {
	try {
	    if (commitLogStream != null) {
		commitLogStream.close();
		commitLogStream = null;
		if (!storage.delete(commitLogFile)) {
		    throw new IOException("Could not delete " + COMMIT_LOG_NAME + " file.");
		}
	    }
	    commitLogStream = new DataOutputStream(storage.create(commitLogFile), bufferSize);
	} catch (IOException e) {
	    throw new StorageException("Could not create empty " + COMMIT_LOG_NAME + ".", e);
	}
    }

    private void runCompaction(File commitLogFile) throws StorageException {
	Compactor compactor = new Compactor(storage, columnFamilyDescriptor, commitLogFile, bufferSize, maxDataFileSize,
		maxGenerations);
	compactor.runCompaction();
	index.update();
    }

    @Override
    public ColumnMap get(byte[] rowKey) throws StorageException {
	ColumnMap columnMap = memtable.get(rowKey);
	if (columnMap != null) {
	    return columnMap;
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
	for (File commitLog : storage.list(columnFamilyDescriptor.getDirectory(), new CommitLogFilenameFilter())) {
	    SSTableReader reader = new SSTableReader(storage, commitLog);
	    ColumnMap entry = reader.readColumnMap(rowKey);
	    if (entry != null) {
		return entry;
	    }
	}
	return null;
    }

    public void setMaxCommitLogSize(int maxCommitLogSize) {
	this.maxCommitLogSize = maxCommitLogSize;
    }

    public void setMaxDataFileSize(long maxDataFileSize) {
	this.maxDataFileSize = maxDataFileSize;
    }
}

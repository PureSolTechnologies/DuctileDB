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
import com.puresoltechnologies.ductiledb.storage.engine.index.Index;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexFactory;
import com.puresoltechnologies.ductiledb.storage.engine.io.CountingOutputStream;
import com.puresoltechnologies.ductiledb.storage.engine.io.commitlog.CommitLogEntry;
import com.puresoltechnologies.ductiledb.storage.engine.io.commitlog.CommitLogIterable;
import com.puresoltechnologies.ductiledb.storage.engine.io.commitlog.CommitLogReader;
import com.puresoltechnologies.ductiledb.storage.engine.io.commitlog.CommitLogWriter;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.SSTableWriter;
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

    public static final String DB_FILE_PREFIX = "DB";
    public static final String DATA_FILE_SUFFIX = ".data";
    public static final String INDEX_FILE_SUFFIX = ".index";
    public static final String MD5_FILE_SUFFIX = ".md5";
    public static final String METADATA_SUFFIX = ".metadata";
    public static final String COMMIT_LOG_PREFIX = "CommitLog";
    public static final String COMMIT_LOG_NAME = COMMIT_LOG_PREFIX + ".failsave";

    public static File getIndexName(File dataFile) {
	return new File(dataFile.getParent(),
		dataFile.getName().replace(ColumnFamilyEngine.DATA_FILE_SUFFIX, ColumnFamilyEngine.INDEX_FILE_SUFFIX));
    }

    public static File getMD5Name(File dataFile) {
	return new File(dataFile.getParent(),
		dataFile.getName().replace(ColumnFamilyEngine.DATA_FILE_SUFFIX, ColumnFamilyEngine.MD5_FILE_SUFFIX));
    }

    private final ExecutorService compactionExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
	@Override
	public Thread newThread(Runnable r) {
	    return new Thread(r, "ductiledb-compaction");
	}
    });

    private long maxCommitLogSize;
    private long maxDataFileSize;

    private final Storage storage;
    private final Memtable memtable;
    private final Index index;
    private final ColumnFamilyDescriptor columnFamilyDescriptor;
    private final File commitLogFile;
    private CommitLogWriter commitLogWriter;
    private CountingOutputStream commitLogSizeStream;
    private final int blockSize;
    private final int bufferSize;

    public ColumnFamilyEngine(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor,
	    DatabaseEngineConfiguration configuration) throws StorageException {
	super();
	logger.info("Starting column family engine '" + columnFamilyDescriptor.getName() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	this.storage = storage;
	this.columnFamilyDescriptor = columnFamilyDescriptor;
	this.commitLogFile = new File(columnFamilyDescriptor.getDirectory(), COMMIT_LOG_NAME);
	this.memtable = MemtableFactory.create();
	this.index = IndexFactory.create(storage, columnFamilyDescriptor);
	this.maxCommitLogSize = configuration.getMaxCommitLogSize();
	this.maxDataFileSize = configuration.getMaxDataFileSize();
	this.blockSize = configuration.getStorage().getBlockSize();
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
		openCommitLog();
	    } else {
		createEmptyCommitLog();
	    }
	    index.update();
	} catch (IOException e) {
	    throw new StorageException("Could not initialize column family '" + columnFamilyDescriptor.getName() + "'.",
		    e);
	}
    }

    private void openCommitLog() throws IOException {
	CommitLogReader commitLogReader = new CommitLogReader(storage, commitLogFile, blockSize);
	try (CommitLogIterable commitLogData = commitLogReader.readData()) {
	    for (CommitLogEntry entry : commitLogData) {
		memtable.put(entry.getRowKey(), entry.getKey(), entry.getValue());
	    }
	}
	FileStatus fileStatus = storage.getFileStatus(commitLogFile);
	commitLogSizeStream = new CountingOutputStream(new BufferedOutputStream(storage.append(commitLogFile)),
		fileStatus.getLength());
	commitLogWriter = new CommitLogWriter(commitLogSizeStream);
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
	    throw new StorageException("Could not write " + COMMIT_LOG_NAME + ".", e);
	}
    }

    private void writeMemtable(byte[] rowKey, Map<byte[], byte[]> values) {
	for (Entry<byte[], byte[]> value : values.entrySet()) {
	    memtable.put(rowKey, value.getKey(), value.getValue());
	}
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
		baseFilename, blockSize, bufferSize)) {
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
	    if (commitLogWriter != null) {
		commitLogWriter.close();
		commitLogWriter = null;
		if (commitLogSizeStream != null) {
		    commitLogSizeStream.close();
		    commitLogSizeStream = null;
		}
		if (!storage.delete(commitLogFile)) {
		    throw new IOException("Could not delete " + COMMIT_LOG_NAME + " file.");
		}
	    }
	    commitLogSizeStream = new CountingOutputStream(storage.create(commitLogFile));
	    commitLogWriter = new CommitLogWriter(commitLogSizeStream);
	} catch (IOException e) {
	    throw new StorageException("Could not create empty " + COMMIT_LOG_NAME + ".", e);
	}
    }

    private void runCompaction(File commitLogFile) throws StorageException {
	Compactor compactor = new Compactor(storage, columnFamilyDescriptor, commitLogFile, blockSize, bufferSize,
		maxDataFileSize);
	compactor.runCompaction();
	index.update();
    }

    public Map<byte[], byte[]> get(byte[] rowKey) {
	return memtable.get(rowKey);
    }

    public void setMaxCommitLogSize(int maxCommitLogSize) {
	this.maxCommitLogSize = maxCommitLogSize;
    }

    public void setMaxDataFileSize(long maxDataFileSize) {
	this.maxDataFileSize = maxDataFileSize;
    }
}

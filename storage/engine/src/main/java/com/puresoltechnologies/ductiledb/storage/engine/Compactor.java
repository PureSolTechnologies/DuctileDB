package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.io.MetadataFilenameFilter;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.SSTableDataEntry;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.SSTableDataIterable;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.SSTableReader;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.SSTableWriter;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;
import com.puresoltechnologies.ductiledb.storage.engine.utils.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.utils.StopWatch;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class is responsible for compaction of column families.
 * 
 * @author Rick-Rainer Ludwig
 */
public class Compactor {

    private static final Logger logger = LoggerFactory.getLogger(Compactor.class);

    private static final ByteArrayComparator comparator = ByteArrayComparator.getInstance();

    private final Storage storage;
    private final ColumnFamilyDescriptor columnFamilyDescriptor;
    private final File commitLogFile;
    private final int blockSize;
    private final int bufferSize;
    private final long maxDataFileSize;

    private int fileCount = 0;
    private final Map<Integer, byte[]> startRowKeys = new HashMap<>();
    private final Map<Integer, byte[]> endRowKeys = new HashMap<>();

    public Compactor(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor, File commitLogFile, int blockSize,
	    int bufferSize, long maxDataFileSize) {
	super();
	this.storage = storage;
	this.columnFamilyDescriptor = columnFamilyDescriptor;
	this.commitLogFile = commitLogFile;
	this.blockSize = blockSize;
	this.bufferSize = bufferSize;
	this.maxDataFileSize = maxDataFileSize;
    }

    public void runCompaction() throws StorageException {
	try {
	    logger.info("Start compaction for '" + commitLogFile + "'...");
	    StopWatch stopWatch = new StopWatch();
	    stopWatch.start();
	    String baseFilename = ColumnFamilyEngine.createBaseFilename(ColumnFamilyEngine.DB_FILE_PREFIX);
	    performCompaction(baseFilename);
	    writeMetaData(baseFilename);
	    deleteCommitLogFiles();
	    stopWatch.stop();
	    logger.info("Compaction for '" + commitLogFile + "' finished in " + stopWatch.getMillis() + "ms.");
	} catch (IOException e) {
	    throw new StorageException("Could not run compaction.", e);
	}
    }

    private List<File> findDataFiles() throws IOException {
	List<String> timestamps = new ArrayList<>();
	for (File file : storage.list(columnFamilyDescriptor.getDirectory(), new MetadataFilenameFilter())) {
	    timestamps.add(ColumnFamilyEngineUtils.extractTimestampForMetadataFile(file.getName()));
	}
	List<File> dataFiles = new ArrayList<>();
	if (timestamps.size() > 0) {
	    Collections.sort(timestamps);
	    while (timestamps.size() > 3) {
		String timestamp = timestamps.get(0);
		for (File file : storage.list(columnFamilyDescriptor.getDirectory(), new FilenameFilter() {
		    @Override
		    public boolean accept(File dir, String name) {
			return name.startsWith(ColumnFamilyEngine.DB_FILE_PREFIX + "-" + timestamp);
		    }
		})) {
		    storage.delete(file);
		}
		timestamps.remove(timestamp);
	    }
	    Collections.reverse(timestamps);
	    String lastTimestamp = timestamps.get(0);
	    for (File file : storage.list(columnFamilyDescriptor.getDirectory(), new FilenameFilter() {
		@Override
		public boolean accept(File dir, String name) {
		    return name.startsWith(ColumnFamilyEngine.DB_FILE_PREFIX + "-" + lastTimestamp)
			    && name.endsWith(ColumnFamilyEngine.DATA_FILE_SUFFIX);
		}
	    })) {
		dataFiles.add(file);
	    }
	    Collections.sort(dataFiles);
	}
	return dataFiles;
    }

    private void performCompaction(String baseFilename) throws StorageException, IOException {
	SSTableReader commitLogReader = new SSTableReader(storage, commitLogFile, blockSize);
	try (SSTableDataIterable commitLogData = commitLogReader.readData()) {
	    Iterator<SSTableDataEntry> commitLogIterator = commitLogData.iterator();
	    List<File> dataFiles = findDataFiles();
	    integrateCommitLog(commitLogIterator, dataFiles, baseFilename);
	}
    }

    private void integrateCommitLog(Iterator<SSTableDataEntry> commitLogIterator, List<File> dataFiles,
	    String baseFilename) throws StorageException, IOException {
	SSTableDataEntry commitLogNext = commitLogIterator.next();
	SSTableWriter writer = new SSTableWriter(storage, columnFamilyDescriptor.getDirectory(),
		baseFilename + "-" + fileCount, blockSize, bufferSize);
	try {
	    for (File dataFile : dataFiles) {
		SSTableReader dataReader = new SSTableReader(storage, dataFile, bufferSize);
		byte[] commitLogRowKey = commitLogNext.getRowKey();
		try (SSTableDataIterable data = dataReader.readData()) {
		    for (SSTableDataEntry dataEntry : data) {
			byte[] dataRowKey = dataEntry.getRowKey();
			if (comparator.compare(dataRowKey, commitLogRowKey) == 0) {
			    writer.write(dataRowKey, dataEntry.update(commitLogNext.getColumns()));
			} else if (comparator.compare(dataRowKey, commitLogRowKey) < 0) {
			    writer.write(dataRowKey, dataEntry.getColumns());
			} else {
			    while (comparator.compare(dataRowKey, commitLogRowKey) > 0) {
				writer.write(dataRowKey, commitLogNext.getColumns());
				commitLogNext = commitLogIterator.next();
				commitLogRowKey = commitLogNext.getRowKey();
			    }
			}
			if (writer.getDataFileSize() >= maxDataFileSize) {
			    writer.close();
			    startRowKeys.put(fileCount, writer.getStartRowKey());
			    endRowKeys.put(fileCount, writer.getEndRowKey());
			    fileCount++;
			    writer = new SSTableWriter(storage, columnFamilyDescriptor.getDirectory(),
				    baseFilename + "-" + fileCount, blockSize, bufferSize);
			}
		    }
		}
	    }
	    if (commitLogNext != null) {
		writer.write(commitLogNext);
		while (commitLogIterator.hasNext()) {
		    SSTableDataEntry commitLogEntry = commitLogIterator.next();
		    writer.write(commitLogEntry.getRowKey(), commitLogEntry.getColumns());
		    if (writer.getDataFileSize() >= maxDataFileSize) {
			writer.close();
			startRowKeys.put(fileCount, writer.getStartRowKey());
			endRowKeys.put(fileCount, writer.getEndRowKey());
			fileCount++;
			writer = new SSTableWriter(storage, columnFamilyDescriptor.getDirectory(),
				baseFilename + "-" + fileCount, blockSize, bufferSize);
		    }
		}
	    }
	} finally {
	    writer.close();
	    startRowKeys.put(fileCount, writer.getStartRowKey());
	    endRowKeys.put(fileCount, writer.getEndRowKey());
	}
    }

    private void writeMetaData(String baseFilename) throws IOException {
	try (BufferedOutputStream stream = new BufferedOutputStream(storage.create(
		new File(columnFamilyDescriptor.getDirectory(), baseFilename + ColumnFamilyEngine.METADATA_SUFFIX)),
		blockSize)) {
	    stream.write(Bytes.toBytes(fileCount + 1)); // Number of files
	    for (int fileId = 0; fileId <= fileCount; ++fileId) {
		stream.write(Bytes.toBytes(fileId)); // file id
		byte[] startRowKey = startRowKeys.get(fileId);
		stream.write(Bytes.toBytes(startRowKey.length));
		stream.write(startRowKey);
		byte[] endRowKey = endRowKeys.get(fileId);
		stream.write(Bytes.toBytes(endRowKey.length));
		stream.write(endRowKey);
	    }
	}
    }

    private void deleteCommitLogFiles() {
	storage.delete(ColumnFamilyEngine.getIndexName(commitLogFile));
	storage.delete(ColumnFamilyEngine.getMD5Name(commitLogFile));
	storage.delete(commitLogFile);
    }

}

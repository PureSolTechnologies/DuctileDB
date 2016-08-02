package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.io.MetadataFilenameFilter;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.ColumnFamilyRowIterable;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.IndexEntryIterable;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.SSTableReader;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.SSTableSet;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.SSTableWriter;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class is responsible for compaction of column families.
 * 
 * @author Rick-Rainer Ludwig
 */
public class Compactor {

    private static final Logger logger = LoggerFactory.getLogger(Compactor.class);

    private final Storage storage;
    private final ColumnFamilyDescriptor columnFamilyDescriptor;
    private final File commitLogFile;
    private final int bufferSize;
    private final long maxDataFileSize;
    private final int maxGenerations;

    private int fileCount = 0;
    private final TreeMap<File, List<IndexEntry>> index = new TreeMap<>();

    public Compactor(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor, File commitLogFile, int bufferSize,
	    long maxDataFileSize, int maxGenerations) {
	super();
	this.storage = storage;
	this.columnFamilyDescriptor = columnFamilyDescriptor;
	this.commitLogFile = commitLogFile;
	this.bufferSize = bufferSize;
	this.maxDataFileSize = maxDataFileSize;
	this.maxGenerations = maxGenerations;
    }

    public void runCompaction() throws StorageException {
	try {
	    logger.info("Start compaction for '" + commitLogFile + "'...");
	    StopWatch stopWatch = new StopWatch();
	    stopWatch.start();
	    String baseFilename = ColumnFamilyEngineImpl.createBaseFilename(ColumnFamilyEngine.DB_FILE_PREFIX);
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
	    deleteObsoleteStorageFiles(timestamps);
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

    private void deleteObsoleteStorageFiles(List<String> timestamps) {
	while (timestamps.size() > maxGenerations) {
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
    }

    private void performCompaction(String baseFilename) throws StorageException, IOException {
	SSTableReader commitLogReader = new SSTableReader(storage, commitLogFile);
	try (IndexEntryIterable commitLogIndex = commitLogReader.readIndex()) {
	    Iterator<IndexEntry> commitLogIterator = commitLogIndex.iterator();
	    List<File> dataFiles = findDataFiles();
	    integrateCommitLog(commitLogIterator, commitLogReader, dataFiles, baseFilename);
	}
    }

    private void integrateCommitLog(Iterator<IndexEntry> commitLogIterator, SSTableReader commitLogReader,
	    List<File> dataFiles, String baseFilename) throws StorageException, IOException {
	SSTableWriter writer = new SSTableWriter(storage, columnFamilyDescriptor.getDirectory(), baseFilename,
		bufferSize);
	try {
	    IndexEntry commitLogNext = commitLogIterator.next();
	    for (File dataFile : dataFiles) {
		SSTableReader dataReader = new SSTableReader(storage, dataFile);
		try (ColumnFamilyRowIterable data = dataReader.readData()) {
		    for (ColumnFamilyRow dataEntry : data) {
			RowKey dataRowKey = dataEntry.getRowKey();
			if (dataRowKey.compareTo(commitLogNext.getRowKey()) == 0) {
			    writer = writeCommitLogEntry(commitLogReader, commitLogNext, writer, baseFilename);
			} else if (dataRowKey.compareTo(commitLogNext.getRowKey()) < 0) {
			    writer = writeDataEntry(writer, baseFilename, dataRowKey, dataEntry.getColumnMap());
			} else {
			    while (dataRowKey.compareTo(commitLogNext.getRowKey()) > 0) {
				writer = writeCommitLogEntry(commitLogReader, commitLogNext, writer, baseFilename);
				commitLogNext = commitLogIterator.next();
			    }
			}
		    }
		}
	    }
	    if (commitLogNext != null) {
		writer = writeCommitLogEntry(commitLogReader, commitLogNext, writer, baseFilename);
		while (commitLogIterator.hasNext()) {
		    commitLogNext = commitLogIterator.next();
		    writer = writeCommitLogEntry(commitLogReader, commitLogNext, writer, baseFilename);
		    if (writer.getDataFileSize() >= maxDataFileSize) {
			writer.close();
			addToIndex(writer);
			fileCount++;
			writer = new SSTableWriter(storage, columnFamilyDescriptor.getDirectory(),
				baseFilename + "-" + fileCount, bufferSize);
		    }
		}
	    }
	} finally {
	    writer.close();
	    addToIndex(writer);
	}
    }

    private SSTableWriter writeCommitLogEntry(SSTableReader commitLogReader, IndexEntry commitLogNext,
	    SSTableWriter writer, String baseFilename) throws IOException, StorageException {
	if (commitLogNext.getOffset() >= 0) {
	    /*
	     * if index is smaller than zero, it is a delete marker, so we skip
	     * the entry to delete it
	     */
	    ColumnMap columnMap = commitLogReader.readColumnMap(commitLogNext);
	    RowKey rowKey = commitLogNext.getRowKey();
	    writer = writeDataEntry(writer, baseFilename, rowKey, columnMap);
	}
	return writer;
    }

    private SSTableWriter writeDataEntry(SSTableWriter writer, String baseFilename, RowKey rowKey, ColumnMap columnMap)
	    throws IOException, StorageException {
	writer.write(rowKey, columnMap);
	if (writer.getDataFileSize() >= maxDataFileSize) {
	    writer.close();
	    addToIndex(writer);
	    fileCount++;
	    writer = new SSTableWriter(storage, columnFamilyDescriptor.getDirectory(), baseFilename, bufferSize);
	}
	return writer;
    }

    private void addToIndex(SSTableWriter writer) {
	List<IndexEntry> indizes = index.get(writer.getDataFile());
	if (indizes == null) {
	    indizes = new ArrayList<>();
	    index.put(writer.getDataFile(), indizes);
	}
	indizes.add(new IndexEntry(writer.getStartRowKey(), writer.getDataFile(), writer.getStartOffset()));
	indizes.add(new IndexEntry(writer.getEndRowKey(), writer.getDataFile(), writer.getEndOffset()));
    }

    private void writeMetaData(String baseFilename) throws IOException {
	try (BufferedOutputStream stream = storage.create(
		new File(columnFamilyDescriptor.getDirectory(), baseFilename + ColumnFamilyEngine.METADATA_SUFFIX))) {
	    stream.write(Bytes.toBytes(fileCount + 1)); // Number of files
	    for (File file : index.keySet()) {
		String fileName = file.getName();
		stream.write(Bytes.toBytes(fileName.length()));
		stream.write(Bytes.toBytes(fileName));
		List<IndexEntry> indexEntries = index.get(file);
		Collections.sort(indexEntries);
		for (IndexEntry entry : indexEntries) {
		    RowKey rowKey = entry.getRowKey();
		    stream.write(Bytes.toBytes(rowKey.getKey().length));
		    stream.write(rowKey.getKey());

		    long offset = entry.getOffset();
		    stream.write(Bytes.toBytes(offset));
		}
	    }
	}
    }

    private void deleteCommitLogFiles() {
	storage.delete(SSTableSet.getIndexName(commitLogFile));
	storage.delete(SSTableSet.getMD5Name(commitLogFile));
	storage.delete(commitLogFile);
    }

}

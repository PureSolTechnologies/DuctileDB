package com.puresoltechnologies.ductiledb.logstore;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.logstore.data.DataFileReader;
import com.puresoltechnologies.ductiledb.logstore.data.DataFileSet;
import com.puresoltechnologies.ductiledb.logstore.index.IndexEntry;
import com.puresoltechnologies.ductiledb.logstore.index.IndexEntryIterator;
import com.puresoltechnologies.ductiledb.logstore.index.IndexFileReader;
import com.puresoltechnologies.ductiledb.logstore.io.SSTableWriter;
import com.puresoltechnologies.ductiledb.logstore.io.filter.MetadataFilenameFilter;
import com.puresoltechnologies.ductiledb.logstore.utils.LogStoreUtils;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class is responsible for compaction of column families.
 * 
 * @author Rick-Rainer Ludwig
 */
public class Compactor {

    private static final Logger logger = LoggerFactory.getLogger(Compactor.class);

    public static void run(Storage storage, File directory, File commitLogFile, int bufferSize, long maxDataFileSize,
	    int maxFileGenerations) {
	Compactor compactor = new Compactor(storage, directory, commitLogFile, bufferSize, maxDataFileSize,
		maxFileGenerations);
	compactor.runCompaction();
    }

    private final Storage storage;
    private final File directory;
    private final File commitLogFile;
    private final int bufferSize;
    private final long maxDataFileSize;
    private final int maxFileGenerations;

    private int fileCount = 0;
    private final TreeMap<File, List<IndexEntry>> index = new TreeMap<>();

    private Compactor(Storage storage, File directory, File commitLogFile, int bufferSize, long maxDataFileSize,
	    int maxFileGenerations) {
	super();
	this.storage = storage;
	this.directory = directory;
	this.commitLogFile = commitLogFile;
	this.bufferSize = bufferSize;
	this.maxDataFileSize = maxDataFileSize;
	this.maxFileGenerations = maxFileGenerations;
    }

    public void runCompaction() {
	try {
	    String baseFilename = LogStructuredStore.createBaseFilename(LogStructuredStore.DB_FILE_PREFIX);
	    logger.info("Start compaction for '" + commitLogFile + "' (new: " + baseFilename + ")...");
	    StopWatch stopWatch = new StopWatch();
	    stopWatch.start();
	    performCompaction(baseFilename);
	    writeMetaData(baseFilename);
	    stopWatch.stop();
	    logger.info("Compaction for '" + commitLogFile + "' (new: " + baseFilename + ") finished in "
		    + stopWatch.getMillis() + "ms.");
	} catch (IOException e) {
	    throw new StorageException("Could not run compaction.", e);
	}
    }

    private List<File> findDataFiles() throws IOException {
	List<String> timestamps = new ArrayList<>();
	for (File file : storage.list(directory, new MetadataFilenameFilter())) {
	    timestamps.add(LogStoreUtils.extractTimestampForMetadataFile(file.getName()));
	}
	List<File> dataFiles = new ArrayList<>();
	if (timestamps.size() > 0) {
	    Collections.sort(timestamps);
	    deleteObsoleteStorageFiles(timestamps);
	    Collections.reverse(timestamps);
	    String lastTimestamp = timestamps.get(0);
	    for (File file : storage.list(directory, new FilenameFilter() {
		@Override
		public boolean accept(File dir, String name) {
		    return name.startsWith(LogStructuredStore.DB_FILE_PREFIX + "-" + lastTimestamp)
			    && name.endsWith(LogStructuredStore.DATA_FILE_SUFFIX);
		}
	    })) {
		dataFiles.add(file);
	    }
	    Collections.sort(dataFiles);
	}
	return dataFiles;
    }

    private void deleteObsoleteStorageFiles(List<String> timestamps) {
	while (timestamps.size() > maxFileGenerations) {
	    String timestamp = timestamps.get(0);
	    for (File file : storage.list(directory, new FilenameFilter() {
		@Override
		public boolean accept(File dir, String name) {
		    return name.startsWith(LogStructuredStore.DB_FILE_PREFIX + "-" + timestamp);
		}
	    })) {
		storage.delete(file);
	    }
	    timestamps.remove(timestamp);
	}
    }

    private void performCompaction(String baseFilename) throws IOException {
	logger.info("Compacting " + commitLogFile + "' (new: " + baseFilename + ")...");
	File indexFile = DataFileSet.getIndexName(commitLogFile);
	try (DataFileReader commitLogReader = new DataFileReader(storage, commitLogFile);
		IndexFileReader commitLogIndex = new IndexFileReader(storage, indexFile)) {
	    IndexEntryIterator commitLogIndexIterator = commitLogIndex.iterator();
	    List<File> dataFiles = findDataFiles();
	    integrateCommitLog(commitLogIndexIterator, commitLogReader, dataFiles, baseFilename);
	}
    }

    private void integrateCommitLog(IndexEntryIterator commitLogIterator, DataFileReader commitLogReader,
	    List<File> dataFiles, String baseFilename) throws IOException {
	SSTableWriter writer = new SSTableWriter(storage, directory, baseFilename + "-" + fileCount, bufferSize);
	try {
	    IndexEntry commitLogNext = commitLogIterator.next();
	    for (File dataFile : dataFiles) {
		try (DataFileReader rows = new DataFileReader(storage, dataFile)) {
		    for (Row row : rows) {
			Key dataRowKey = row.getKey();
			if (commitLogNext != null) {
			    if (dataRowKey.compareTo(commitLogNext.getRowKey()) == 0) {
				writer = writeCommitLogEntry(commitLogReader, commitLogNext, writer, baseFilename);
				commitLogNext = commitLogIterator.next();
			    } else if (dataRowKey.compareTo(commitLogNext.getRowKey()) < 0) {
				writer = writeDataEntry(writer, baseFilename, dataRowKey, row.getTombstone(),
					row.getData());
			    } else {
				if (commitLogNext != null) {
				    writer = writeCommitLogEntry(commitLogReader, commitLogNext, writer, baseFilename);
				    commitLogNext = commitLogIterator.next();
				}
			    }
			} else {
			    writer = writeDataEntry(writer, baseFilename, dataRowKey, row.getTombstone(),
				    row.getData());
			}
		    }
		}
	    }
	    if (commitLogNext != null) {
		writer = writeCommitLogEntry(commitLogReader, commitLogNext, writer, baseFilename);
		while (commitLogIterator.hasNext()) {
		    commitLogNext = commitLogIterator.next();
		    writer = writeCommitLogEntry(commitLogReader, commitLogNext, writer, baseFilename);
		}
	    }
	} catch (Exception e) {
	    logger.error("Could not integrate commit log.", e);
	    throw e;
	} finally {
	    writer.close();
	    addToIndex(writer);
	}
    }

    private SSTableWriter writeCommitLogEntry(DataFileReader commitLogReader, IndexEntry commitLogNext,
	    SSTableWriter writer, String baseFilename) throws IOException, StorageException {
	Row row = commitLogReader.readRow(commitLogNext);
	if (!row.wasDeleted()) {
	    byte[] data = row.getData();
	    if (data != null) {
		writer = writeDataEntry(writer, baseFilename, row.getKey(), null, data);
	    }

	}
	return writer;
    }

    private SSTableWriter writeDataEntry(SSTableWriter writer, String baseFilename, Key rowKey, Instant tombstone,
	    byte[] data) throws IOException, StorageException {
	writer.write(rowKey, tombstone, data);
	if (writer.getDataFileSize() >= maxDataFileSize) {
	    writer.close();
	    addToIndex(writer);
	    fileCount++;
	    writer = new SSTableWriter(storage, directory, baseFilename + "-" + fileCount, bufferSize);
	}
	return writer;
    }

    private void addToIndex(SSTableWriter writer) {
	List<IndexEntry> indizes = index.get(writer.getDataFile());
	if (indizes == null) {
	    indizes = new ArrayList<>();
	    index.put(writer.getDataFile(), indizes);
	}
	if (writer.hasIndexInformation()) {
	    indizes.add(new IndexEntry(writer.getStartRowKey(), writer.getDataFile(), writer.getStartOffset()));
	    indizes.add(new IndexEntry(writer.getEndRowKey(), writer.getDataFile(), writer.getEndOffset()));
	}
    }

    private void writeMetaData(String baseFilename) throws IOException {
	logger.info("Creating meta data for " + commitLogFile + "' (new: " + baseFilename + ")...");
	try (BufferedOutputStream stream = storage
		.create(new File(directory, baseFilename + LogStructuredStore.METADATA_SUFFIX))) {
	    stream.write(Bytes.fromInt(fileCount + 1)); // Number of files
	    for (File file : index.keySet()) {
		String fileName = file.getName();
		stream.write(Bytes.fromInt(fileName.length()));
		stream.write(Bytes.fromString(fileName));
		List<IndexEntry> indexEntries = index.get(file);
		Collections.sort(indexEntries);
		for (IndexEntry entry : indexEntries) {
		    Key rowKey = entry.getRowKey();
		    stream.write(Bytes.fromInt(rowKey.getBytes().length));
		    stream.write(rowKey.getBytes());

		    long offset = entry.getOffset();
		    stream.write(Bytes.fromLong(offset));
		}
	    }
	} catch (Exception e) {
	    logger.error("Could not write meta data.", e);
	    throw e;
	}
    }

}

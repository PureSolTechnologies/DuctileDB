package com.puresoltechnologies.ductiledb.logstore.io;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.LogStructuredStore;
import com.puresoltechnologies.ductiledb.logstore.Row;
import com.puresoltechnologies.ductiledb.logstore.index.Index;
import com.puresoltechnologies.ductiledb.logstore.index.IndexEntry;
import com.puresoltechnologies.ductiledb.logstore.index.IndexFactory;
import com.puresoltechnologies.ductiledb.logstore.index.IndexIterator;
import com.puresoltechnologies.ductiledb.logstore.index.OffsetRange;
import com.puresoltechnologies.ductiledb.logstore.index.io.IndexFileReader;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class DataFileSet implements Closeable {

    private static final Pattern pattern = Pattern.compile("DB-(\\d*)-(\\d+)\\.data");
    private static final Logger logger = LoggerFactory.getLogger(DataFileSet.class);

    public static File getIndexName(File dataFile) {
	return new File(dataFile.getParent(),
		dataFile.getName().replace(LogStructuredStore.DATA_FILE_SUFFIX, LogStructuredStore.INDEX_FILE_SUFFIX));
    }

    public static File getDeletedName(File dataFile) {
	return new File(dataFile.getParent(), dataFile.getName().replace(LogStructuredStore.DATA_FILE_SUFFIX,
		LogStructuredStore.DELETED_FILE_SUFFIX));
    }

    public static File getMD5Name(File dataFile) {
	return new File(dataFile.getParent(),
		dataFile.getName().replace(LogStructuredStore.DATA_FILE_SUFFIX, LogStructuredStore.MD5_FILE_SUFFIX));
    }

    private static File getMetadataFile(File directory, String timestamp) {
	return new File(directory,
		LogStructuredStore.DB_FILE_PREFIX + "-" + timestamp + LogStructuredStore.DATA_FILE_SUFFIX);
    }

    public static File getLatestMetaDataFile(Storage storage, File directory) {
	Iterable<File> listMetadata = storage.list(directory, new MetadataFilenameFilter());
	File latestMetadata = null;
	for (File metadata : listMetadata) {
	    if ((latestMetadata == null) || (latestMetadata.compareTo(metadata) < 0)) {
		latestMetadata = metadata;
	    }
	}
	return latestMetadata;
    }

    private final Storage storage;
    private final File metadataFile;
    private final Index index;
    private final NavigableSet<File> dataFiles = new TreeSet<>();
    private final NavigableSet<File> indexFiles = new TreeSet<>();
    private final NavigableMap<File, IndexFileReader> indexReaders = new TreeMap<>();
    private final NavigableMap<File, DataFileReader> dataReaders = new TreeMap<>();

    private final Map<Integer, File> numToIndexFile = new HashMap<>();
    private final Map<File, Integer> indexFileToNum = new HashMap<>();

    public DataFileSet(Storage storage, File directory) {
	this(storage, directory, getLatestMetaDataFile(storage, directory));
    }

    public DataFileSet(Storage storage, File directory, File metadataFile) {
	super();
	this.storage = storage;
	this.metadataFile = metadataFile;
	this.index = IndexFactory.create(storage, directory, metadataFile);

	index.forEach(indexEntry -> dataFiles.add(indexEntry.getDataFile()));
	dataFiles.forEach(file -> {
	    Matcher matcher = pattern.matcher(file.getName());
	    if (matcher.matches()) {
		File indexName = getIndexName(file);
		indexFiles.add(indexName);
		int num = Integer.parseInt(matcher.group(2));
		numToIndexFile.put(num, indexName);
		indexFileToNum.put(indexName, num);
	    } else {
		logger.error("Invalid data file found: " + file);
	    }
	});
    }

    public DataFileSet(Storage storage, File directory, String timestamp) {
	this(storage, directory, getMetadataFile(directory, timestamp));
    }

    Index getIndex() {
	return index;
    }

    Storage getStorage() {
	return storage;
    }

    @Override
    public void close() throws IOException {
	dataReaders.values().forEach(stream -> {
	    try {
		stream.close();
	    } catch (IOException e) {
		logger.warn("Could not close data stream.", e);
	    }
	});
	dataReaders.clear();
	indexReaders.values().forEach(stream -> {
	    try {
		stream.close();
	    } catch (IOException e) {
		logger.warn("Could not close index stream.", e);
	    }
	});
	indexReaders.clear();
    }

    public void check() {
	// TODO Auto-generated method stub

    }

    public Row getRow(Key rowKey) throws IOException {
	OffsetRange offsetRange = index.find(rowKey);
	if (offsetRange == null) {
	    return null;
	}
	IndexEntry startOffset = offsetRange.getStartOffset();
	IndexEntry endOffset = offsetRange.getEndOffset();
	File dataFile = startOffset.getDataFile();
	File dataFile2 = endOffset.getDataFile();
	if (!dataFile.equals(dataFile2)) {
	    throw new IllegalStateException(
		    "File overlapping index range for key '" + rowKey + "':\n" + startOffset + "\n" + endOffset);
	}
	IndexEntry indexEntry = findEntry(rowKey, dataFile);
	if (indexEntry == null) {
	    return null;
	}
	return readRow(dataFile, indexEntry);
    }

    private IndexEntry findEntry(Key rowKey, File dataFile) throws IOException {
	IndexEntry indexEntry;
	File indexFile = getIndexName(dataFile);
	IndexFileReader indexReader = indexReaders.get(indexFile);
	if (indexReader == null) {
	    synchronized (indexReaders) {
		indexReader = indexReaders.get(indexFile);
		if (indexReader == null) {
		    try {
			indexReader = new IndexFileReader(storage, indexFile);
			indexReaders.put(indexFile, indexReader);
		    } catch (FileNotFoundException e) {
			logger.warn("Could not find index file.", e);
			indexEntry = null;
		    }
		}
	    }
	}
	synchronized (indexReader) {
	    indexReader.goToOffset(0);
	    indexEntry = indexReader.get(rowKey);
	}
	return indexEntry;
    }

    private Row readRow(File dataFile, IndexEntry indexEntry) throws IOException {
	DataFileReader dataReader = dataReaders.get(dataFile);
	if (dataReader == null) {
	    synchronized (dataReaders) {
		dataReader = dataReaders.get(dataFile);
		if (dataReader == null) {
		    try {
			dataReader = new DataFileReader(storage, dataFile);
			dataReaders.put(dataFile, dataReader);
		    } catch (FileNotFoundException e) {
			logger.warn("Could not find data file.", e);
			return null;
		    }
		}
	    }
	}
	Row row;
	synchronized (dataReader) {
	    dataReader.goToOffset(indexEntry.getOffset());
	    row = dataReader.getRow();
	}
	return row;
    }

    File getNextIndexFile(File indexFile) {
	if (indexFile == null) {
	    return null;
	}
	Integer num = indexFileToNum.get(indexFile);
	return num != null ? numToIndexFile.get(num + 1) : null;
    }

    public IndexIterator getIndexIterator(Key startRowKey, Key stopRowKey) throws IOException {
	return new SSTableIndexIterator(this, startRowKey, stopRowKey);
    }
}

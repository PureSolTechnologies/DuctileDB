package com.puresoltechnologies.ductiledb.engine.io;

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

import com.puresoltechnologies.ductiledb.engine.Key;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.engine.cf.index.primary.Index;
import com.puresoltechnologies.ductiledb.engine.cf.index.primary.IndexEntry;
import com.puresoltechnologies.ductiledb.engine.cf.index.primary.IndexFactory;
import com.puresoltechnologies.ductiledb.engine.cf.index.primary.IndexIterator;
import com.puresoltechnologies.ductiledb.engine.cf.index.primary.OffsetRange;
import com.puresoltechnologies.ductiledb.engine.cf.index.primary.io.IndexFileReader;
import com.puresoltechnologies.ductiledb.engine.cf.io.DataFileReader;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class DataFileSet implements Closeable {

    private static final Pattern pattern = Pattern.compile("DB-(\\d*)-(\\d+)\\.data");
    private static final Logger logger = LoggerFactory.getLogger(DataFileSet.class);

    public static File getIndexName(File dataFile) {
	return new File(dataFile.getParent(), dataFile.getName().replace(ColumnFamilyEngineImpl.DATA_FILE_SUFFIX,
		ColumnFamilyEngineImpl.INDEX_FILE_SUFFIX));
    }

    public static File getDeletedName(File dataFile) {
	return new File(dataFile.getParent(), dataFile.getName().replace(ColumnFamilyEngineImpl.DATA_FILE_SUFFIX,
		ColumnFamilyEngineImpl.DELETED_FILE_SUFFIX));
    }

    public static File getMD5Name(File dataFile) {
	return new File(dataFile.getParent(), dataFile.getName().replace(ColumnFamilyEngineImpl.DATA_FILE_SUFFIX,
		ColumnFamilyEngineImpl.MD5_FILE_SUFFIX));
    }

    private static File getMetadataFile(File directory, String timestamp) {
	return new File(directory,
		ColumnFamilyEngineImpl.DB_FILE_PREFIX + "-" + timestamp + ColumnFamilyEngineImpl.DATA_FILE_SUFFIX);
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

    public ColumnFamilyRow getRow(Key rowKey) throws IOException {
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

	File indexFile = getIndexName(dataFile);
	IndexFileReader indexReader = indexReaders.get(indexFile);
	if (indexReader == null) {
	    try {
		indexReader = new IndexFileReader(storage, indexFile);
		indexReaders.put(indexFile, indexReader);
	    } catch (FileNotFoundException e) {
		logger.warn("Could not find index file.", e);
		dataReaders.remove(indexFile);
		return null;
	    }
	}
	indexReader.goToOffset(0);
	IndexEntry indexEntry = indexReader.get(rowKey);
	if (indexEntry == null) {
	    return null;
	}

	DataFileReader dataReader = dataReaders.get(dataFile);
	if (dataReader == null) {
	    try {
		dataReader = new DataFileReader(storage, dataFile);
		dataReaders.put(dataFile, dataReader);
	    } catch (FileNotFoundException e) {
		logger.warn("Could not find data file.", e);
		dataReaders.remove(dataFile);
		return null;
	    }
	}
	dataReader.goToOffset(indexEntry.getOffset());
	return dataReader.getRow();
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
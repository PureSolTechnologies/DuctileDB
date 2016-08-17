package com.puresoltechnologies.ductiledb.storage.engine.io;

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

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.storage.engine.index.Index;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexFactory;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexIterator;
import com.puresoltechnologies.ductiledb.storage.engine.index.OffsetRange;
import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.io.data.DataFileReader;
import com.puresoltechnologies.ductiledb.storage.engine.io.index.IndexEntryIterable;
import com.puresoltechnologies.ductiledb.storage.engine.io.index.IndexFileReader;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
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

    private static File getMetadataFile(ColumnFamilyDescriptor columnFamilyDescriptor, String timestamp) {
	return new File(columnFamilyDescriptor.getDirectory(),
		ColumnFamilyEngineImpl.DB_FILE_PREFIX + "-" + timestamp + ColumnFamilyEngineImpl.DATA_FILE_SUFFIX);
    }

    public static File getLatestMetaDataFile(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor) {
	Iterable<File> listMetadata = storage.list(columnFamilyDescriptor.getDirectory(), new MetadataFilenameFilter());
	File latestMetadata = null;
	for (File metadata : listMetadata) {
	    System.out.println("MetaData:" + metadata);
	    if ((latestMetadata == null) || (latestMetadata.compareTo(metadata) < 0)) {
		System.out.println("new:" + metadata);
		latestMetadata = metadata;
	    }
	}
	return latestMetadata;
    }

    private final Storage storage;
    private final ColumnFamilyDescriptor columnFamilyDescriptor;
    private final File metadataFile;
    private final Index index;
    private final NavigableSet<File> dataFiles = new TreeSet<>();
    private final NavigableSet<File> indexFiles = new TreeSet<>();
    private final NavigableMap<File, IndexFileReader> indexReaders = new TreeMap<>();
    private final NavigableMap<File, DataFileReader> dataReaders = new TreeMap<>();

    private final Map<Integer, File> numToIndexFile = new HashMap<>();
    private final Map<File, Integer> indexFileToNum = new HashMap<>();

    public DataFileSet(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor) throws StorageException {
	this(storage, columnFamilyDescriptor, getLatestMetaDataFile(storage, columnFamilyDescriptor));
    }

    public DataFileSet(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor, File metadataFile)
	    throws StorageException {
	super();
	this.storage = storage;
	this.columnFamilyDescriptor = columnFamilyDescriptor;
	this.metadataFile = metadataFile;
	this.index = IndexFactory.create(storage, columnFamilyDescriptor, metadataFile);

	index.forEach(indexEntry -> dataFiles.add(indexEntry.getDataFile()));
	dataFiles.forEach(file -> {
	    Matcher matcher = pattern.matcher(file.getName());
	    if (matcher.matches()) {
		indexFiles.add(getIndexName(file));
		int num = Integer.parseInt(matcher.group(2));
		numToIndexFile.put(num, file);
		indexFileToNum.put(file, num);
	    } else {
		logger.error("Invalid data file found: " + file);
	    }
	});
    }

    public DataFileSet(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor, String timestamp)
	    throws StorageException {
	this(storage, columnFamilyDescriptor, getMetadataFile(columnFamilyDescriptor, timestamp));
    }

    public ColumnFamilyDescriptor getColumnFamilyDescriptor() {
	return columnFamilyDescriptor;
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

    public ColumnFamilyRow getRow(RowKey rowKey) throws IOException {
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

    private File getNextIndexFile(File indexFile) {
	if (indexFile == null) {
	    return null;
	}
	Integer num = indexFileToNum.get(indexFile);
	return num != null ? numToIndexFile.get(num + 1) : null;
    }

    private class SSTableIndexIterator implements IndexIterator {

	private final RowKey start;
	private final RowKey stop;
	private File currentDataFile;
	private File currentIndexFile;
	private IndexEntryIterable indexIterable;
	private IndexIterator indexIterator;
	private IndexEntry nextIndex = null;

	public SSTableIndexIterator(RowKey start, RowKey stop) throws IOException {
	    this.start = start;
	    this.stop = stop;
	    IndexEntry floor = index.floor(start);
	    if (floor != null) {
		currentDataFile = floor.getDataFile();
	    } else {
		IndexEntry ceiling = index.ceiling(start);
		if (ceiling != null) {
		    currentDataFile = ceiling.getDataFile();
		}
	    }
	    if (currentDataFile != null) {
		currentIndexFile = getIndexName(currentDataFile);
		indexIterable = new IndexEntryIterable(storage.open(currentIndexFile));
		indexIterator = indexIterable.iterator(start, stop);
		gotoStart(start);
	    }
	}

	@Override
	public RowKey getStartRowKey() {
	    return start;
	}

	@Override
	public RowKey getEndRowKey() {
	    return stop;
	}

	@Override
	public IndexEntry peek() {
	    if (nextIndex == null) {
		readNext();
	    }
	    return nextIndex;
	}

	@Override
	public boolean hasNext() {
	    if (nextIndex == null) {
		readNext();
	    }
	    return nextIndex != null;
	}

	@Override
	public IndexEntry next() {
	    if (nextIndex == null) {
		readNext();
	    }
	    IndexEntry result = nextIndex;
	    nextIndex = null;
	    return result;
	}

	private void readNext() {
	    if (indexIterator == null) {
		nextIndex = null;
		return;
	    }
	    if (indexIterator.hasNext()) {
		nextIndex = indexIterator.next();
	    } else {
		try {
		    try {
			indexIterable.close();
		    } finally {
			indexIterable = null;
			indexIterator = null;
		    }
		    currentIndexFile = getNextIndexFile(currentIndexFile);
		    if (currentIndexFile != null) {
			indexIterable = new IndexEntryIterable(storage.open(currentIndexFile));
			indexIterator = indexIterable.iterator(start, stop);
			nextIndex = indexIterator.next();
		    }
		} catch (IOException e) {
		    logger.error("Could not find next index file.", e);
		    nextIndex = null;
		}
	    }
	}

	@Override
	public void close() throws IOException {
	    if (indexIterable != null) {
		indexIterable.close();
		indexIterable = null;
	    }
	}

    }

    public IndexIterator getIndexIterator(RowKey startRowKey, RowKey stopRowKey) throws IOException {
	return new SSTableIndexIterator(startRowKey, stopRowKey);
    }
}

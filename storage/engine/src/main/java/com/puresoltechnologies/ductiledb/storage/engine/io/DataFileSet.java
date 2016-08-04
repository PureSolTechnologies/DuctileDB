package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.engine.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.index.Index;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexFactory;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexIterator;
import com.puresoltechnologies.ductiledb.storage.engine.index.OffsetRange;
import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.io.data.DataFileReader;
import com.puresoltechnologies.ductiledb.storage.engine.io.index.IndexEntryIterable;
import com.puresoltechnologies.ductiledb.storage.engine.io.index.IndexFileReader;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class DataFileSet implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(DataFileSet.class);

    public static File getIndexName(File dataFile) {
	return new File(dataFile.getParent(), dataFile.getName().replace(ColumnFamilyEngineImpl.DATA_FILE_SUFFIX,
		ColumnFamilyEngineImpl.INDEX_FILE_SUFFIX));
    }

    public static File getMD5Name(File dataFile) {
	return new File(dataFile.getParent(), dataFile.getName().replace(ColumnFamilyEngineImpl.DATA_FILE_SUFFIX,
		ColumnFamilyEngineImpl.MD5_FILE_SUFFIX));
    }

    public static File getLatestMetaDataFile(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor) {
	Iterable<File> listMetadata = storage.list(columnFamilyDescriptor.getDirectory(), new MetadataFilenameFilter());
	File latestMetadata = null;
	for (File metadata : listMetadata) {
	    if ((latestMetadata == null) || (latestMetadata.compareTo(metadata) < 0)) {
		latestMetadata = metadata;
	    }
	}
	return latestMetadata;
    }

    private final Storage storage;
    private final ColumnFamilyDescriptor columnFamilyDescriptor;
    private final String timestamp;
    private final Index index;
    private final NavigableSet<File> dataFiles = new TreeSet<>();
    private final NavigableMap<File, IndexFileReader> indexReaders = new TreeMap<>();
    private final NavigableMap<File, DataFileReader> dataReaders = new TreeMap<>();

    public DataFileSet(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor) throws FileNotFoundException {
	this(storage, columnFamilyDescriptor, getLatestMetaDataFile(storage, columnFamilyDescriptor));
    }

    public DataFileSet(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor, File metadataFile)
	    throws FileNotFoundException {
	this(storage, columnFamilyDescriptor, metadataFile != null
		? metadataFile.getName().replaceAll("DB-", "").replaceAll("\\.metadata", "") : null);
    }

    public DataFileSet(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor, String timestamp)
	    throws FileNotFoundException {
	this.storage = storage;
	this.columnFamilyDescriptor = columnFamilyDescriptor;
	this.timestamp = timestamp;
	this.index = IndexFactory.create(storage, columnFamilyDescriptor);
	for (File dataFile : storage.list(columnFamilyDescriptor.getDirectory(), new DataFilenameFilter(timestamp))) {
	    dataFiles.add(dataFile);
	}
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

    public ColumnMap get(RowKey rowKey) throws IOException {
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

	DataFileReader dataReader = dataReaders.get(dataFile);
	if (dataReader == null) {
	    dataReader = new DataFileReader(storage, dataFile);
	    dataReaders.put(dataFile, dataReader);
	}
	dataReader.goToOffset(startOffset.getOffset());
	return dataReader.get();
    }

    private class SSTableIndexIterator implements IndexIterator {

	private final RowKey start;
	private final RowKey stop;
	private File currentDataFile;
	private File currentIndexFile;
	private IndexEntryIterable indexIterable;
	private IndexIterator indexIterator;

	public SSTableIndexIterator(RowKey start, RowKey stop) throws FileNotFoundException {
	    this.start = start;
	    this.stop = stop;
	    currentDataFile = index.floor(start).getDataFile();
	    currentIndexFile = getIndexName(currentDataFile);
	    indexIterable = new IndexEntryIterable(currentIndexFile, storage.open(currentIndexFile));
	    indexIterator = indexIterable.iterator(start, stop);
	    gotoStart(start);
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
	    return indexIterator.peek();
	}

	@Override
	public boolean hasNext() {
	    if (!indexIterator.hasNext()) {

	    }
	    return false;
	}

	@Override
	public IndexEntry next() {
	    return indexIterator.next();
	}

	@Override
	public void close() throws IOException {
	    if (indexIterable != null) {
		indexIterable.close();
	    }
	}

    }

    public IndexIterator getIndexIterator(RowKey startRowKey, RowKey stopRowKey) throws FileNotFoundException {
	return new SSTableIndexIterator(startRowKey, stopRowKey);
    }
}

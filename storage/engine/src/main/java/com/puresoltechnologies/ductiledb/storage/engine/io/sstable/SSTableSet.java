package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.engine.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.index.Index;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexFactory;
import com.puresoltechnologies.ductiledb.storage.engine.index.OffsetRange;
import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.io.MetadataFilenameFilter;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class SSTableSet implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(SSTableSet.class);

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
    private final Map<File, IndexFileReader> indexReaders = new HashMap<>();
    private final Map<File, DataFileReader> dataReaders = new HashMap<>();

    public SSTableSet(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor) throws FileNotFoundException {
	this(storage, columnFamilyDescriptor, getLatestMetaDataFile(storage, columnFamilyDescriptor));
    }

    public SSTableSet(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor, File metadataFile)
	    throws FileNotFoundException {
	this(storage, columnFamilyDescriptor, metadataFile != null
		? metadataFile.getName().replaceAll("DB-", "").replaceAll("\\.metadata", "") : null);
    }

    public SSTableSet(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor, String timestamp)
	    throws FileNotFoundException {
	this.storage = storage;
	this.columnFamilyDescriptor = columnFamilyDescriptor;
	this.timestamp = timestamp;
	this.index = IndexFactory.create(storage, columnFamilyDescriptor);
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
}

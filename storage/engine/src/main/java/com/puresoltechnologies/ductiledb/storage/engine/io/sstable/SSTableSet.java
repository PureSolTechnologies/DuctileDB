package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

import java.io.File;

import com.puresoltechnologies.ductiledb.storage.engine.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class SSTableSet {

    public static File getIndexName(File dataFile) {
	return new File(dataFile.getParent(), dataFile.getName().replace(ColumnFamilyEngineImpl.DATA_FILE_SUFFIX,
		ColumnFamilyEngineImpl.INDEX_FILE_SUFFIX));
    }

    public static File getMD5Name(File dataFile) {
	return new File(dataFile.getParent(), dataFile.getName().replace(ColumnFamilyEngineImpl.DATA_FILE_SUFFIX,
		ColumnFamilyEngineImpl.MD5_FILE_SUFFIX));
    }

    private final Storage storage;
    private final String timestamp;

    public SSTableSet(Storage storage, String timestamp) {
	this.storage = storage;
	this.timestamp = timestamp;
    }

    public SSTableSet(Storage storage, File metadataFile) {
	this.storage = storage;
	this.timestamp = metadataFile.getName().replaceAll("DB-", "").replaceAll("\\.metadata", "");
    }

    public void check() {
	// TODO Auto-generated method stub

    }

}

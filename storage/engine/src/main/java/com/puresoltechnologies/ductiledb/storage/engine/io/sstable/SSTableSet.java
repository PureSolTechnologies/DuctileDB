package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

import java.io.File;

import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class SSTableSet {

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

package com.puresoltechnologies.ductiledb.storage.engine.cf.io;

import com.puresoltechnologies.ductiledb.storage.engine.Key;

public class MetaDataEntry {

    private final String fileName;
    private final Key startKey;
    private final long startOffset;
    private final Key endKey;
    private final long endOffset;

    public MetaDataEntry(String fileName, Key startKey, long startOffset, Key endKey, long endOffset) {
	super();
	this.fileName = fileName;
	this.startKey = startKey;
	this.startOffset = startOffset;
	this.endKey = endKey;
	this.endOffset = endOffset;
    }

    public String getFileName() {
	return fileName;
    }

    public Key getStartKey() {
	return startKey;
    }

    public long getStartOffset() {
	return startOffset;
    }

    public Key getEndKey() {
	return endKey;
    }

    public long getEndOffset() {
	return endOffset;
    }

    public boolean isEmptyDataFile() {
	return (startOffset < 0) && (endOffset < 0);
    }

}

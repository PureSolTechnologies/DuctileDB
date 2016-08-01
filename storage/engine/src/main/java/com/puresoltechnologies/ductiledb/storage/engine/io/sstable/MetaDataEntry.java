package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;

public class MetaDataEntry {

    private final String fileName;
    private final RowKey startKey;
    private final long startOffset;
    private final RowKey endKey;
    private final long endOffset;

    public MetaDataEntry(String fileName, RowKey startKey, long startOffset, RowKey endKey, long endOffset) {
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

    public RowKey getStartKey() {
	return startKey;
    }

    public long getStartOffset() {
	return startOffset;
    }

    public RowKey getEndKey() {
	return endKey;
    }

    public long getEndOffset() {
	return endOffset;
    }

}

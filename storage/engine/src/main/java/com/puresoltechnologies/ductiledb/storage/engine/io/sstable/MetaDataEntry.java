package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

public class MetaDataEntry {

    private final String fileName;
    private final byte[] startKey;
    private final long startOffset;
    private final byte[] endKey;
    private final long endOffset;

    public MetaDataEntry(String fileName, byte[] startKey, long startOffset, byte[] endKey, long endOffset) {
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

    public byte[] getStartKey() {
	return startKey;
    }

    public long getStartOffset() {
	return startOffset;
    }

    public byte[] getEndKey() {
	return endKey;
    }

    public long getEndOffset() {
	return endOffset;
    }

}

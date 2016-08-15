package com.puresoltechnologies.ductiledb.storage.engine.index;

import java.io.File;
import java.util.Objects;

/**
 * This class provides the index result.
 * 
 * @author Rick-Rainer Ludwig
 */
public class IndexEntry implements Comparable<IndexEntry> {

    private final RowKey rowKey;
    private final File dataFile;
    private final long offset;
    private final int hashCode;

    public IndexEntry(RowKey rowKey, File dataFile, long offset) {
	super();
	this.rowKey = rowKey;
	this.dataFile = dataFile;
	this.offset = offset;
	this.hashCode = Objects.hash(rowKey, dataFile, offset);
    }

    public RowKey getRowKey() {
	return rowKey;
    }

    public File getDataFile() {
	return dataFile;
    }

    public long getOffset() {
	return offset;
    }

    @Override
    public int compareTo(IndexEntry o) {
	return this.rowKey.compareTo(o.rowKey);
    }

    @Override
    public int hashCode() {
	return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	IndexEntry other = (IndexEntry) obj;
	if (hashCode != other.hashCode)
	    return false;
	if (offset != other.offset)
	    return false;
	if (dataFile == null) {
	    if (other.dataFile != null)
		return false;
	} else if (!dataFile.equals(other.dataFile))
	    return false;
	if (rowKey == null) {
	    if (other.rowKey != null)
		return false;
	} else if (!rowKey.equals(other.rowKey))
	    return false;
	return true;
    }

    @Override
    public String toString() {
	return rowKey + " -> " + dataFile + ":" + offset;
    }

    public boolean wasDeleted() {
	return offset < 0;
    }
}

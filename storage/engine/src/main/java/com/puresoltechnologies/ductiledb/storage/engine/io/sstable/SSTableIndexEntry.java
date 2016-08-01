package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;

/**
 * This class represents a single index file entry.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class SSTableIndexEntry {

    private final RowKey rowKey;
    private final long offset;

    public SSTableIndexEntry(RowKey rowKey, long offset) {
	super();
	this.rowKey = rowKey;
	this.offset = offset;
    }

    public RowKey getRowKey() {
	return rowKey;
    }

    public long getOffset() {
	return offset;
    }

}

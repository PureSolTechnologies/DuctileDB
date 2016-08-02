package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.File;
import java.util.List;

import com.puresoltechnologies.commons.misc.PeekingIterator;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.SSTableSet;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.Memtable;

public class ColumnFamilyScanner implements PeekingIterator<IndexEntry> {

    private final PeekingIterator<IndexEntry> memtableIterator;
    private final List<File> commitLogs;
    private final SSTableSet dataFiles;
    private final RowKey startRowKey;
    private final RowKey endRowKey;

    public ColumnFamilyScanner(Memtable memtable, List<File> commitLogs, SSTableSet dataFiles, RowKey startRowKey,
	    RowKey endRowKey) {
	super();
	this.memtableIterator = memtable.iterator(startRowKey, endRowKey);
	this.commitLogs = commitLogs;
	this.dataFiles = dataFiles;
	this.startRowKey = startRowKey;
	this.endRowKey = endRowKey;

    }

    @Override
    public boolean hasNext() {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public IndexEntry next() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public IndexEntry peek() {
	// TODO Auto-generated method stub
	return null;
    }

}

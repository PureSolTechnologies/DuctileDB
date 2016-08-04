package com.puresoltechnologies.ductiledb.storage.engine.index;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

import com.puresoltechnologies.commons.misc.PeekingIterator;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.io.DataFileSet;
import com.puresoltechnologies.ductiledb.storage.engine.io.DuctileDBInputStream;
import com.puresoltechnologies.ductiledb.storage.engine.io.data.MetaDataEntry;
import com.puresoltechnologies.ductiledb.storage.engine.io.data.MetaDataEntryIterable;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.trees.RedBlackTree;
import com.puresoltechnologies.trees.RedBlackTreeNode;

public class IndexImpl implements Index {

    private final Storage storage;
    private final ColumnFamilyDescriptor columnFamilyDescriptor;
    private final RedBlackTree<RowKey, IndexEntry> indexTree = new RedBlackTree<>();

    public IndexImpl(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor) {
	super();
	this.storage = storage;
	this.columnFamilyDescriptor = columnFamilyDescriptor;
    }

    public IndexImpl(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor, File metadataFile)
	    throws StorageException {
	super();
	this.storage = storage;
	this.columnFamilyDescriptor = columnFamilyDescriptor;
	update(metadataFile);
    }

    @Override
    public void update() throws StorageException {
	update(DataFileSet.getLatestMetaDataFile(storage, columnFamilyDescriptor));
    }

    @Override
    public void update(File latestMetadata) throws StorageException {
	indexTree.clear();
	if (latestMetadata == null) {
	    return;
	}
	try {
	    for (MetaDataEntry entry : new MetaDataEntryIterable(
		    new DuctileDBInputStream(storage.open(latestMetadata)))) {
		IndexEntry index1 = new IndexEntry(entry.getStartKey(),
			new File(columnFamilyDescriptor.getDirectory(), entry.getFileName()), entry.getStartOffset());
		IndexEntry index2 = new IndexEntry(entry.getEndKey(),
			new File(columnFamilyDescriptor.getDirectory(), entry.getFileName()), entry.getEndOffset());
		if (storage.exists(index1.getDataFile())) {
		    indexTree.put(index1.getRowKey(), index1);
		}
		if (storage.exists(index2.getDataFile())) {
		    indexTree.put(index2.getRowKey(), index2);
		}
	    }
	} catch (IOException e) {
	    throw new StorageException("Could not determine latest timestamp.");
	}
    }

    @Override
    public void put(byte[] rowKey, IndexEntry indexEntry) {
	indexTree.put(new RowKey(rowKey), indexEntry);
    }

    @Override
    public IndexEntry get(byte[] rowKey) {
	return indexTree.get(new RowKey(rowKey));
    }

    @Override
    public OffsetRange find(RowKey rowKey) {
	try {
	    RedBlackTreeNode<RowKey, IndexEntry> floorNode = indexTree.floorNode(rowKey);
	    RedBlackTreeNode<RowKey, IndexEntry> ceilingNode = indexTree.ceilingNode(rowKey);
	    if ((floorNode == null) || (ceilingNode == null)) {
		return null;
	    }
	    return new OffsetRange(floorNode.getValue(), ceilingNode.getValue());
	} catch (NoSuchElementException e) {
	    return null;
	}
    }

    @Override
    public IndexEntry ceiling(RowKey rowKey) {
	return indexTree.ceilingNode(rowKey).getValue();
    }

    @Override
    public IndexEntry floor(RowKey rowKey) {
	return indexTree.floorNode(rowKey).getValue();
    }

    @Override
    public PeekingIterator<IndexEntry> iterator() {
	return new PeekingIterator<IndexEntry>() {

	    private PeekingIterator<RedBlackTreeNode<RowKey, IndexEntry>> iterator = indexTree.iterator();

	    @Override
	    public boolean hasNext() {
		return iterator.hasNext();
	    }

	    @Override
	    public IndexEntry peek() {
		return iterator.peek().getValue();
	    }

	    @Override
	    public IndexEntry next() {
		return iterator.next().getValue();
	    }

	    @Override
	    public void remove() {
		iterator.remove();
	    }
	};

    }
}

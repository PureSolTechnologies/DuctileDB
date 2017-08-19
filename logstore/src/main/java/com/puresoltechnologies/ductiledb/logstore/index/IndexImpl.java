package com.puresoltechnologies.ductiledb.logstore.index;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.io.DataFileSet;
import com.puresoltechnologies.ductiledb.logstore.io.DuctileDBInputStream;
import com.puresoltechnologies.ductiledb.logstore.io.MetaDataEntry;
import com.puresoltechnologies.ductiledb.logstore.io.MetaDataEntryIterable;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.streaming.StreamIterator;
import com.puresoltechnologies.trees.RedBlackTree;
import com.puresoltechnologies.trees.RedBlackTreeNode;

public class IndexImpl implements Index {

    private final Storage storage;
    private final File directory;
    private final RedBlackTree<Key, IndexEntry> indexTree = new RedBlackTree<>();

    IndexImpl(Storage storage, File directory) {
	super();
	this.storage = storage;
	this.directory = directory;
    }

    IndexImpl(Storage storage, File directory, File metadataFile) {
	super();
	this.storage = storage;
	this.directory = directory;
	update(metadataFile);
    }

    @Override
    public void update() {
	update(DataFileSet.getLatestMetaDataFile(storage, directory));
    }

    @Override
    public void update(File latestMetadata) {
	indexTree.clear();
	if (latestMetadata == null) {
	    return;
	}
	try (MetaDataEntryIterable metaDataEntryIterable = new MetaDataEntryIterable(
		new DuctileDBInputStream(storage.open(latestMetadata)));) {
	    for (MetaDataEntry entry : metaDataEntryIterable) {
		IndexEntry index1 = new IndexEntry(entry.getStartKey(), new File(directory, entry.getFileName()),
			entry.getStartOffset());
		IndexEntry index2 = new IndexEntry(entry.getEndKey(), new File(directory, entry.getFileName()),
			entry.getEndOffset());
		if (!entry.isEmptyDataFile()) {
		    // We avoid empty data files here.
		    if (storage.exists(index1.getDataFile())) {
			indexTree.put(index1.getRowKey(), index1);
		    }
		    if (storage.exists(index2.getDataFile())) {
			indexTree.put(index2.getRowKey(), index2);
		    }
		}
	    }
	} catch (IOException e) {
	    throw new StorageException("Could not determine latest timestamp.", e);
	}
    }

    @Override
    public void put(IndexEntry indexEntry) {
	indexTree.put(indexEntry.getRowKey(), indexEntry);
    }

    @Override
    public IndexEntry get(byte[] rowKey) {
	return indexTree.get(Key.of(rowKey));
    }

    @Override
    public OffsetRange find(Key rowKey) {
	try {
	    RedBlackTreeNode<Key, IndexEntry> floorNode = indexTree.floorNode(rowKey);
	    RedBlackTreeNode<Key, IndexEntry> ceilingNode = indexTree.ceilingNode(rowKey);
	    if ((floorNode == null) || (ceilingNode == null)) {
		return null;
	    }
	    return new OffsetRange(floorNode.getValue(), ceilingNode.getValue());
	} catch (NoSuchElementException e) {
	    return null;
	}
    }

    @Override
    public IndexEntry ceiling(Key rowKey) {
	if (rowKey == null) {
	    return indexTree.isEmpty() ? null : indexTree.get(indexTree.min());
	}
	RedBlackTreeNode<Key, IndexEntry> ceilingNode = indexTree.ceilingNode(rowKey);
	return ceilingNode != null ? ceilingNode.getValue() : null;
    }

    @Override
    public IndexEntry floor(Key rowKey) {
	if (rowKey == null) {
	    return indexTree.isEmpty() ? null : indexTree.get(indexTree.min());
	}
	RedBlackTreeNode<Key, IndexEntry> floorNode = indexTree.floorNode(rowKey);
	return floorNode != null ? floorNode.getValue() : null;
    }

    @Override
    public IndexIterator iterator() {
	return new IndexIterator() {

	    private StreamIterator<RedBlackTreeNode<Key, IndexEntry>> iterator = indexTree.iterator();

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

	    @Override
	    public void close() throws IOException {
		// intentionally left empty
	    }

	    @Override
	    public Key getStartRowKey() {
		return null;
	    }

	    @Override
	    public Key getEndRowKey() {
		return null;
	    }
	};

    }
}

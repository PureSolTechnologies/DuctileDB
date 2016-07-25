package com.puresoltechnologies.ductiledb.storage.engine.index;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.io.MetadataFilenameFilter;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.MetaDataEntry;
import com.puresoltechnologies.ductiledb.storage.engine.io.sstable.MetaDataEntryIterable;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.trees.RedBlackTree;

public class IndexImpl implements Index {

    private static final ByteArrayComparator comparator = ByteArrayComparator.getInstance();

    private static class RowKey implements Comparable<RowKey> {

	private final byte[] rowKey;

	public RowKey(byte[] rowKey) {
	    super();
	    this.rowKey = rowKey;
	}

	@Override
	public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + Arrays.hashCode(rowKey);
	    return result;
	}

	@Override
	public boolean equals(Object obj) {
	    if (this == obj)
		return true;
	    if (obj == null)
		return false;
	    if (getClass() != obj.getClass())
		return false;
	    RowKey other = (RowKey) obj;
	    if (!Arrays.equals(rowKey, other.rowKey))
		return false;
	    return true;
	}

	@Override
	public int compareTo(RowKey o) {
	    return comparator.compare(rowKey, o.rowKey);
	}

    }

    private final Storage storage;
    private final ColumnFamilyDescriptor columnFamilyDescriptor;

    private final RedBlackTree<RowKey, IndexEntry> indexTree = new RedBlackTree<>();

    public IndexImpl(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor) {
	super();
	this.storage = storage;
	this.columnFamilyDescriptor = columnFamilyDescriptor;
    }

    @Override
    public void update() throws StorageException {
	try {
	    Iterable<File> listMetadata = storage.list(columnFamilyDescriptor.getDirectory(),
		    new MetadataFilenameFilter());
	    File latestMetadata = null;
	    for (File metadata : listMetadata) {
		if ((latestMetadata == null) || (latestMetadata.compareTo(metadata) < 0)) {
		    latestMetadata = metadata;
		}
	    }
	    if (latestMetadata != null) {
		for (MetaDataEntry entry : new MetaDataEntryIterable(storage.open(latestMetadata))) {
		    IndexEntry index1 = new IndexEntry(entry.getStartKey(),
			    new File(columnFamilyDescriptor.getDirectory(), entry.getFileName()),
			    entry.getStartOffset());
		    IndexEntry index2 = new IndexEntry(entry.getEndKey(),
			    new File(columnFamilyDescriptor.getDirectory(), entry.getFileName()), entry.getEndOffset());
		    indexTree.put(new RowKey(index1.getRowKey()), index1);
		    indexTree.put(new RowKey(index2.getRowKey()), index2);
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
    public OffsetRange find(byte[] rowKey) {
	try {
	    return new OffsetRange(indexTree.floorNode(new RowKey(rowKey)).getValue(),
		    indexTree.ceilingNode(new RowKey(rowKey)).getValue());
	} catch (NoSuchElementException e) {
	    return null;
	}
    }
}

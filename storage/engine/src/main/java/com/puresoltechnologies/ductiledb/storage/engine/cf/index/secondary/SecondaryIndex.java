package com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary;

import java.util.Set;
import java.util.TreeSet;

import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class SecondaryIndex {

    private final Storage storage;
    private final ColumnFamilyDescriptor descriptor;
    private final TreeSet<byte[]> columns = new TreeSet<>(ByteArrayComparator.getInstance());

    public SecondaryIndex(Storage storage, ColumnFamilyDescriptor descriptor, Set<byte[]> columns) {
	super();
	this.storage = storage;
	this.descriptor = descriptor;
	this.columns.addAll(columns);
    }

}

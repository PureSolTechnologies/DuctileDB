package com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary;

import java.io.File;
import java.util.Set;
import java.util.TreeSet;

import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;

/**
 * This class contains the definition of a Secondary Index for a column family.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class SecondaryIndexDescriptor {

    private final String name;
    private final ColumnFamilyDescriptor columnFamilyDescriptor;
    private final TreeSet<byte[]> columns = new TreeSet<>(ByteArrayComparator.getInstance());

    public SecondaryIndexDescriptor(String name, ColumnFamilyDescriptor columnFamilyDescriptor, Set<byte[]> columns) {
	super();
	this.name = name;
	this.columnFamilyDescriptor = columnFamilyDescriptor;
	this.columns.addAll(columns);
    }

    public final String getName() {
	return name;
    }

    public final ColumnFamilyDescriptor getColumnFamilyDescriptor() {
	return columnFamilyDescriptor;
    }

    public final TreeSet<byte[]> getColumns() {
	return columns;
    }

    public File getDirectory() {
	return new File(columnFamilyDescriptor.getDirectory(), name);
    }

}

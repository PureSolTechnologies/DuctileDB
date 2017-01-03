package com.puresoltechnologies.ductiledb.bigtable.cf.index;

import java.io.File;
import java.util.Set;

import com.puresoltechnologies.ductiledb.bigtable.cf.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.bigtable.cf.ColumnKeySet;
import com.puresoltechnologies.ductiledb.logstore.Key;

/**
 * This class contains the definition of a Secondary Index for a column family.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class SecondaryIndexDescriptor {

    private final String name;
    private final ColumnFamilyDescriptor columnFamilyDescriptor;
    private final ColumnKeySet columns = new ColumnKeySet();
    private final IndexType indexType;

    public SecondaryIndexDescriptor(String name, ColumnFamilyDescriptor columnFamilyDescriptor, Set<Key> columns,
	    IndexType indexType) {
	super();
	this.name = name;
	this.columnFamilyDescriptor = columnFamilyDescriptor;
	this.columns.addAll(columns);
	this.indexType = indexType;
    }

    public final String getName() {
	return name;
    }

    public final ColumnFamilyDescriptor getColumnFamilyDescriptor() {
	return columnFamilyDescriptor;
    }

    public final ColumnKeySet getColumns() {
	return columns;
    }

    public IndexType getIndexType() {
	return indexType;
    }

    public File getDirectory() {
	return new File(new File(columnFamilyDescriptor.getDirectory(), "indizes"), name);
    }

    public boolean matchesColumns(Key... columns) {
	return matchesColumns(new ColumnKeySet(columns));
    }

    public boolean matchesColumns(ColumnKeySet columnSet) {
	if (columns.size() > columnSet.size()) {
	    return false;
	}
	for (Key column : columns) {
	    if (!columnSet.contains(column)) {
		return false;
	    }
	}
	return true;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((columnFamilyDescriptor == null) ? 0 : columnFamilyDescriptor.hashCode());
	result = prime * result + ((columns == null) ? 0 : columns.hashCode());
	result = prime * result + ((indexType == null) ? 0 : indexType.hashCode());
	result = prime * result + ((name == null) ? 0 : name.hashCode());
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
	SecondaryIndexDescriptor other = (SecondaryIndexDescriptor) obj;
	if (columnFamilyDescriptor == null) {
	    if (other.columnFamilyDescriptor != null)
		return false;
	} else if (!columnFamilyDescriptor.equals(other.columnFamilyDescriptor))
	    return false;
	if (columns == null) {
	    if (other.columns != null)
		return false;
	} else if (!columns.equals(other.columns))
	    return false;
	if (indexType != other.indexType)
	    return false;
	if (name == null) {
	    if (other.name != null)
		return false;
	} else if (!name.equals(other.name))
	    return false;
	return true;
    }
}

package com.puresoltechnologies.ductiledb.bigtable;

import java.util.Set;

import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyMap;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnMap;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnValue;
import com.puresoltechnologies.ductiledb.logstore.Key;

public class Put {

    private final ColumnFamilyMap columnFamilies = new ColumnFamilyMap();
    private final Key rowKey;

    public Put(Key rowKey) {
	super();
	this.rowKey = rowKey;
    }

    public final Key getKey() {
	return rowKey;
    }

    public final Set<Key> getColumnFamilies() {
	return columnFamilies.keySet();
    }

    public final ColumnMap getColumnValues(Key columnFamily) {
	return columnFamilies.get(columnFamily);
    }

    public final void addColumn(Key columnFamilyName, Key key, ColumnValue value) {
	ColumnMap columnFamily = columnFamilies.get(columnFamilyName);
	if (columnFamily == null) {
	    columnFamily = new ColumnMap();
	    columnFamilies.put(columnFamilyName, columnFamily);
	}
	columnFamily.put(key, value);
    }

}

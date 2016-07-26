package com.puresoltechnologies.ductiledb.storage.engine.memtable;

/**
 * Basic Memtable implementation.
 * 
 * @author Rick-Rainer Ludwig
 */
public class Memtable {

    private final RowMap values = new RowMap();

    public Memtable() {
	super();
    }

    public void clear() {
	values.clear();
    }

    public void put(byte[] rowKey, byte[] key, byte[] value) {
	ColumnMap row = values.get(rowKey);
	if (row == null) {
	    row = new ColumnMap();
	    values.put(rowKey, row);
	}
	row.put(key, value);
    }

    public ColumnMap get(byte[] rowKey) {
	return values.get(rowKey);
    }

    public RowMap getValues() {
	return values;
    }

}

package com.puresoltechnologies.ductiledb.storage.engine.memtable;

/**
 * Basic Memtable implementation.
 * 
 * @author Rick-Rainer Ludwig
 */
public class MemtableImpl implements Memtable {

    private final RowMap values = new RowMap();

    public MemtableImpl() {
	super();
    }

    @Override
    public void clear() {
	values.clear();
    }

    @Override
    public void put(byte[] rowKey, byte[] key, byte[] value) {
	ColumnMap row = values.get(rowKey);
	if (row == null) {
	    row = new ColumnMap();
	    values.put(rowKey, row);
	}
	row.put(key, value);
    }

    @Override
    public ColumnMap get(byte[] rowKey) {
	return values.get(rowKey);
    }

    @Override
    public RowMap getValues() {
	return values;
    }

}

package com.puresoltechnologies.ductiledb.storage.engine.memtable;

/**
 * This interface represents a memtable used for storage engine.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface Memtable {

    public void put(byte[] rowKey, byte[] key, byte[] value);

    public ColumnMap get(byte[] rowKey);

    public void clear();

    public RowMap getValues();

}

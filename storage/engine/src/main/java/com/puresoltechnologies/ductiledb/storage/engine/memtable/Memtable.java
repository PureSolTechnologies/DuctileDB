package com.puresoltechnologies.ductiledb.storage.engine.memtable;

import java.util.Map;

/**
 * This interface represents a memtable used for storage engine.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface Memtable {

    public void put(byte[] rowKey, byte[] key, byte[] value);

    public Map<byte[], byte[]> get(byte[] rowKey);

    public void clear();

    public Map<byte[], Map<byte[], byte[]>> getValues();

}

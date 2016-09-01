package com.puresoltechnologies.ductiledb.storage.engine.cf;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary.SecondaryIndexDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.lss.LogStructuredStore;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;

/**
 * This class handles the storage of a single column family.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public interface ColumnFamilyEngine extends LogStructuredStore {

    public byte[] getName();

    public ColumnFamilyDescriptor getDescriptor();

    /**
     * Increments (or decrements) a value in atomic way.
     * 
     * @param rowKey
     * @param column
     * @param incrementValue
     * @throws StorageException
     */
    public long incrementColumnValue(byte[] rowKey, byte[] column, long incrementValue) throws StorageException;

    /**
     * Increments (or decrements) a value in atomic way.
     * 
     * @param rowKey
     * @param column
     * @param startValue
     * @param incrementValue
     * @throws StorageException
     */
    public long incrementColumnValue(byte[] rowKey, byte[] column, long startValue, long incrementValue)
	    throws StorageException;

    public void createIndex(SecondaryIndexDescriptor indexDescriptor) throws StorageException;

    public void dropIndex(String name);

    public SecondaryIndexDescriptor getIndex(String name);

    public Iterable<SecondaryIndexDescriptor> getIndizes();
}

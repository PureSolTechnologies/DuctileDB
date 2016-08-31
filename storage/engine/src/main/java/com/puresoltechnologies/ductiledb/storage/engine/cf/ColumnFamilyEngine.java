package com.puresoltechnologies.ductiledb.storage.engine.cf;

import java.io.Closeable;
import java.util.Set;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SecondaryIndexDescriptor;

/**
 * This class handles the storage of a single column family.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public interface ColumnFamilyEngine extends Closeable {

    public static final String DB_FILE_PREFIX = "DB";
    public static final String DATA_FILE_SUFFIX = ".data";
    public static final String INDEX_FILE_SUFFIX = ".index";
    public static final String DELETED_FILE_SUFFIX = ".delete";
    public static final String COMPACTED_FILE_SUFFIX = ".compacted";
    public static final String MD5_FILE_SUFFIX = ".md5";
    public static final String METADATA_SUFFIX = ".metadata";
    public static final String COMMIT_LOG_PREFIX = "CommitLog";

    public byte[] getName();

    public ColumnFamilyDescriptor getDescriptor();

    /**
     * This method is used to put additional columns to the given row.
     * 
     * @param rowKey
     * @param columnValues
     * @throws StorageException
     */
    public void put(byte[] rowKey, ColumnMap columnValues) throws StorageException;

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

    /**
     * This metho retrieves the columns from the given row.
     * 
     * @param rowKey
     * @return
     * @throws StorageException
     */
    public ColumnMap get(byte[] rowKey) throws StorageException;

    /**
     * This method removes the given row.
     * 
     * @param rowKey
     * @throws StorageException
     */
    public void delete(byte[] rowKey) throws StorageException;

    public void delete(byte[] rowKey, Set<byte[]> columns) throws StorageException;

    /**
     * This method returns a scanner for the column family.
     * 
     * @return
     */
    public ColumnFamilyScanner getScanner(byte[] startRowKey, byte[] endRowKey) throws StorageException;

    public void createIndex(SecondaryIndexDescriptor indexDescriptor);

    public void dropIndex(String name);

    public SecondaryIndexDescriptor getIndex(String name);

    public Iterable<SecondaryIndexDescriptor> getIndizes();
}

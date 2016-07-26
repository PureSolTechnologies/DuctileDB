package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.Closeable;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.ColumnMap;

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
    public static final String MD5_FILE_SUFFIX = ".md5";
    public static final String METADATA_SUFFIX = ".metadata";
    public static final String COMMIT_LOG_PREFIX = "CommitLog";
    public static final String COMMIT_LOG_NAME = COMMIT_LOG_PREFIX + ".failsave";

    public void put(byte[] timestamp, byte[] rowKey, ColumnMap columnValues) throws StorageException;

    public ColumnMap get(byte[] rowKey) throws StorageException;

}

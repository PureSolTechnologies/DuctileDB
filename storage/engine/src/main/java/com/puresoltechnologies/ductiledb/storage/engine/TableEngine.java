package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.Closeable;
import java.util.List;
import java.util.Set;

import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyEngine;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;

/**
 * This class is the central engine class for table storage. It is using the
 * {@link ColumnFamilyEngine} to store the separated column families.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface TableEngine extends Closeable {

    public void addColumnFamily(ColumnFamilyDescriptor columnFamilyDescriptor);

    public void dropColumnFamily(ColumnFamilyDescriptor columnFamilyDescriptor);

    public Set<byte[]> getColumnFamilies();

    public void put(Put put);

    public void put(List<Put> puts);

    public void delete(Delete delete);

    public void delete(List<Delete> deletes);

    public Result get(Get get);

    public ResultScanner getScanner(Scan scan);

    public ResultScanner find(Scan scan, byte[] columnKey, byte[] value);

    public ResultScanner find(Scan scan, byte[] columnKey, byte[] fromValue, byte[] toValue);

    public long incrementColumnValue(byte[] rowKey, byte[] columnFamily, byte[] column, long incrementValue);
}

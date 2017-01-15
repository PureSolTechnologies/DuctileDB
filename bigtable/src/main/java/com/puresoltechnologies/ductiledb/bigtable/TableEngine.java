package com.puresoltechnologies.ductiledb.bigtable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.puresoltechnologies.ductiledb.bigtable.cf.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.bigtable.cf.ColumnFamilyEngine;
import com.puresoltechnologies.ductiledb.bigtable.cf.ColumnValue;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class is the central engine class for table storage. It is using the
 * {@link ColumnFamilyEngine} to store the separated column families.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface TableEngine extends Closeable {

    public static TableEngine reopen(Storage storage, File directory) throws IOException {
	return new TableEngineImpl(storage, directory);
    }

    public static TableEngine create(Storage storage, TableDescriptor tableDescriptor,
	    BigTableEngineConfiguration configuration) throws IOException {
	return new TableEngineImpl(storage, tableDescriptor, configuration);
    }

    public void addColumnFamily(ColumnFamilyDescriptor columnFamilyDescriptor) throws IOException;

    public void dropColumnFamily(ColumnFamilyDescriptor columnFamilyDescriptor);

    public Set<Key> getColumnFamilies();

    public void put(Put put);

    public void put(List<Put> puts);

    public void delete(Delete delete);

    public void delete(List<Delete> deletes);

    public Result get(Get get);

    public ResultScanner getScanner(Scan scan);

    public ResultScanner find(Scan scan, Key columnKey, ColumnValue value);

    public ResultScanner find(Scan scan, Key columnKey, ColumnValue fromValue, ColumnValue toValue);

    public long incrementColumnValue(Key rowKey, Key columnFamily, Key column, long incrementValue);
}

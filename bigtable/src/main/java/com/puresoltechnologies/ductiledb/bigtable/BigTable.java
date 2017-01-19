package com.puresoltechnologies.ductiledb.bigtable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamily;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnValue;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class is the central engine class for table storage. It is using the
 * {@link ColumnFamily} to store the separated column families.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface BigTable extends Closeable {

    public static BigTable reopen(Storage storage, File directory) throws IOException {
	return new BigTableImpl(storage, directory);
    }

    public static BigTable create(Storage storage, TableDescriptor tableDescriptor, BigTableConfiguration configuration)
	    throws IOException {
	return new BigTableImpl(storage, tableDescriptor, configuration);
    }

    public String getName();

    public ColumnFamily addColumnFamily(Key name) throws IOException;

    public void dropColumnFamily(Key name);

    public ColumnFamily getColumnFamily(Key name);

    public boolean hasColumnFamily(Key name);

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

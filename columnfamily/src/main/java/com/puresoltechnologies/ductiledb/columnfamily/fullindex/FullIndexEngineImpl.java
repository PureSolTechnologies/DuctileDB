package com.puresoltechnologies.ductiledb.columnfamily.fullindex;

import java.io.File;
import java.io.IOException;

import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamily;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyImpl;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyScanner;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnValue;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.LogStoreConfiguration;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class FullIndexEngineImpl implements FullIndexEngine {

    private final ColumnFamilyImpl columnFamily;

    public FullIndexEngineImpl(Storage storage, File fullIndexDirectory, LogStoreConfiguration configuration)
	    throws IOException {
	if (storage.exists(fullIndexDirectory)) {
	    columnFamily = (ColumnFamilyImpl) ColumnFamily.reopen(storage, fullIndexDirectory);
	} else {
	    columnFamily = (ColumnFamilyImpl) ColumnFamily.create(storage,
		    new ColumnFamilyDescriptor(Key.of("FullIndex"), fullIndexDirectory), configuration);
	}
    }

    @Override
    public void close() throws IOException {
	columnFamily.close();
    }

    @Override
    public ColumnFamilyScanner find(Key columnKey, ColumnValue value) {

	return null;
    }

    @Override
    public ColumnFamilyScanner find(Key columnKey, ColumnValue fromValue, ColumnValue toValue) {
	// TODO Auto-generated method stub
	return null;
    }

}

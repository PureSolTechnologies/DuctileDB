package com.puresoltechnologies.ductiledb.engine;

import java.io.IOException;

import com.puresoltechnologies.ductiledb.bigtable.BigTable;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamily;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyImpl;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class AbstractColumnFamiliyEngineTest extends AbstractDatabaseEngineTest {

    private DatabaseEngineImpl engine;
    private Namespace namespace;
    private BigTable table;
    private ColumnFamily columnFamily;
    private Storage storage;

    protected ColumnFamilyImpl createTestColumnFamily(String namespaceName, String tableName, String columnFamilyName)
	    throws IOException {
	engine = getEngine();
	if (!engine.hasNamespace(namespaceName)) {
	    namespace = engine.addNamespace(namespaceName);
	} else {
	    namespace = engine.getNamespace(namespaceName);
	}
	if (!namespace.hasTable(tableName)) {
	    table = namespace.addTable(tableName, "description");
	} else {
	    table = namespace.getTable(tableName);
	}
	if (!table.hasColumnFamily(Key.of(columnFamilyName))) {
	    columnFamily = table.addColumnFamily(Key.of(columnFamilyName));
	} else {
	    columnFamily = table.getColumnFamily(Key.of(columnFamilyName));
	}
	storage = engine.getStorage();
	return (ColumnFamilyImpl) columnFamily;
    }

    protected Storage getStorage() {
	return storage;
    }

    protected ColumnFamily getColumnFamily() {
	return columnFamily;
    }

    protected ColumnFamilyDescriptor getColumnFamilyDescriptor() {
	return columnFamily.getDescriptor();
    }
}

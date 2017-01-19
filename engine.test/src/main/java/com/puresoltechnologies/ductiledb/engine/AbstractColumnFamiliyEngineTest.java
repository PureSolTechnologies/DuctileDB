package com.puresoltechnologies.ductiledb.engine;

import java.io.IOException;

import com.puresoltechnologies.ductiledb.bigtable.BigTable;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamily;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyImpl;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class AbstractColumnFamiliyEngineTest extends AbstractDatabaseEngineTest {

    private DatabaseEngineImpl engine;
    private NamespaceEngine namespace;
    private BigTable table;
    private ColumnFamily columnFamily;
    private Storage storage;

    protected ColumnFamilyImpl createTestColumnFamily(String namespaceName, String tableName, String columnFamilyName)
	    throws IOException {
	engine = getEngine();
	if (!engine.hasNamespace(namespaceName)) {
	    namespace = engine.addNamespace(namespaceName);
	}
	if (!namespace.hasTable(tableName)) {
	    table = namespace.addTable(tableName, "description");
	}
	if (!table.hasColumnFamily(Key.of(columnFamilyName))) {
	    columnFamily = table.addColumnFamily(Key.of(columnFamilyName));
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
}

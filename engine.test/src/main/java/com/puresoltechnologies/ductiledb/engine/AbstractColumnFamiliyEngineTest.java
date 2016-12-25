package com.puresoltechnologies.ductiledb.engine;

import com.puresoltechnologies.ductiledb.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.engine.TableEngine;
import com.puresoltechnologies.ductiledb.engine.TableEngineImpl;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnFamilyEngine;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.engine.schema.TableDescriptor;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class AbstractColumnFamiliyEngineTest extends AbstractDatabaseEngineTest {

    private DatabaseEngineImpl engine;
    private SchemaManager schemaManager;
    private NamespaceDescriptor namespace;
    private TableDescriptor tableDescriptor;
    private ColumnFamilyDescriptor columnFamilyDescriptor;
    private Storage storage;
    private TableEngine table;
    private ColumnFamilyEngine columnFamily;

    protected ColumnFamilyEngineImpl createTestColumnFamily(String namespaceName, String tableName,
	    String columnFamilyName) throws SchemaException, StorageException {
	engine = getEngine();
	schemaManager = engine.getSchemaManager();
	namespace = schemaManager.createNamespaceIfNotPresent(namespaceName);
	tableDescriptor = schemaManager.createTableIfNotPresent(namespace, tableName, "");
	columnFamilyDescriptor = schemaManager.createColumnFamilyIfNotPresent(tableDescriptor,
		Key.of(columnFamilyName));
	storage = engine.getStorage();
	table = engine.getTable(tableDescriptor);
	columnFamily = ((TableEngineImpl) table).getColumnFamilyEngine(columnFamilyDescriptor.getName());
	return (ColumnFamilyEngineImpl) columnFamily;
    }

    protected Storage getStorage() {
	return storage;
    }

    protected ColumnFamilyDescriptor getColumnFamilyDescriptor() {
	return columnFamilyDescriptor;
    }
}

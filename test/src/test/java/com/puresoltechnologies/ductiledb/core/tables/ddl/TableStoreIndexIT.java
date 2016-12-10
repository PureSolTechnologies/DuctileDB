package com.puresoltechnologies.ductiledb.core.tables.ddl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.core.tables.AbstractTableStoreTest;
import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnType;
import com.puresoltechnologies.ductiledb.core.tables.schema.TableStoreSchema;

public class TableStoreIndexIT extends AbstractTableStoreTest {

    private static final String NAMESPACE = TableStoreIndexIT.class.getSimpleName();

    @BeforeClass
    public static void createTestnamespace() throws ExecutionException {
	TableStoreImpl tableStore = getTableStore();

	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	CreateNamespace createNamespace = ddl.createCreateNamespace(NAMESPACE);
	createNamespace.bind().execute(tableStore);

	TableStoreSchema schema = tableStore.getSchema();
	NamespaceDefinition namespaceDefinition = schema.getNamespaceDefinition(NAMESPACE);
	assertNotNull(namespaceDefinition);
	assertEquals(NAMESPACE, namespaceDefinition.getName());
    }

    @Test
    public void testIndexCRUD() throws ExecutionException {
	final String TABLE = "testIndexCRUD";
	final String INDEX = "idxTestIndexCRUD";
	final String COLUMN_FAMILY = "default";
	TableStoreImpl tableStore = getTableStore();
	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	CreateTable createTable = ddl.createCreateTable(NAMESPACE, TABLE);
	createTable.addColumn(COLUMN_FAMILY, "column1", ColumnType.VARCHAR);
	createTable.addColumn(COLUMN_FAMILY, "column2", ColumnType.VARCHAR);
	createTable.addColumn(COLUMN_FAMILY, "column3", ColumnType.VARCHAR);
	createTable.setPrimaryKey("column1");
	createTable.bind().execute(tableStore);

	CreateIndex createIndex = ddl.createCreateIndex(NAMESPACE, TABLE, COLUMN_FAMILY, INDEX);
	// TODO
    }
}

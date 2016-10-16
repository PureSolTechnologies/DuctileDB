package com.puresoltechnologies.ductiledb.core.tables.schema;

import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.puresoltechnologies.ductiledb.core.cli.DuctileDBConsoleOutput;
import com.puresoltechnologies.ductiledb.core.tables.AbstractTableStoreTest;
import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;

public class TableStoreSchemaIT extends AbstractTableStoreTest {

    private static final DuctileDBConsoleOutput output = new DuctileDBConsoleOutput(System.out);

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testSystemNamespacesTable() throws IOException, ExecutionException {
	TableStoreImpl tableStore = getTableStore();
	output.printTableContent(tableStore, "system", "namespaces");
    }

    @Test
    public void testSystemTablesTable() throws IOException, ExecutionException {
	TableStoreImpl tableStore = getTableStore();
	output.printTableContent(tableStore, "system", "tables");
    }

    @Test
    public void testSystemColumnsTable() throws IOException, ExecutionException {
	TableStoreImpl tableStore = getTableStore();
	output.printTableContent(tableStore, "system", "columns");
    }
}

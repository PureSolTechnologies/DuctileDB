package com.puresoltechnologies.ductiledb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;

import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnType;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinitionImpl;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowImpl;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterableImpl;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class DuctileResultSetTest {

    @Test
    public void test() throws SQLException {
	DuctileConnection connection = Mockito.mock(DuctileConnection.class);

	TableDefinitionImpl tableDefinition = new TableDefinitionImpl("system", "namespaces");
	tableDefinition.addColumn("metadata", "TABLE_CAT", ColumnType.VARCHAR);

	List<String> list = new ArrayList<>();
	list.add("1");
	list.add("2");
	list.add("3");

	try (DuctileResultSet resultSet = new DuctileResultSet(connection, new TableRowIterableImpl<>(list, catalog -> {
	    TableRowImpl tableRow = new TableRowImpl(tableDefinition, null);
	    tableRow.add("TABLE_CAT", Bytes.toBytes(catalog));
	    return tableRow;
	}))) {
	    int count = 0;
	    List<String> results = new ArrayList<>();
	    while (resultSet.next()) {
		count++;
		results.add(resultSet.getString("TABLE_CAT"));
	    }
	    assertEquals(3, count);
	    assertTrue(results.contains("1"));
	    assertTrue(results.contains("2"));
	    assertTrue(results.contains("3"));
	}

    }

}

package com.puresoltechnologies.ductiledb.core.tables.dml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnType;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinitionImpl;
import com.puresoltechnologies.ductiledb.logstore.utils.Bytes;

public class TableRowIterableImplTest {

    @Test
    public void test() throws IOException {
	TableDefinitionImpl tableDefinition = new TableDefinitionImpl("system", "namespaces", "");
	tableDefinition.addColumn("metadata", "TABLE_CAT", ColumnType.VARCHAR);

	List<String> list = new ArrayList<>();
	list.add("1");
	list.add("2");
	list.add("3");

	try (TableRowIterable tableRowIterable = new TableRowIterableImpl<>(list, s -> {
	    TableRowImpl tableRow = new TableRowImpl(tableDefinition, null);
	    tableRow.add("TABLE_CAT", Bytes.toBytes(s));
	    return tableRow;
	})) {
	    int count = 0;
	    List<String> results = new ArrayList<>();
	    for (TableRow row : tableRowIterable) {
		count++;
		results.add(row.getString("TABLE_CAT"));
	    }
	    assertEquals(3, count);
	    assertTrue(results.contains("1"));
	    assertTrue(results.contains("2"));
	    assertTrue(results.contains("3"));
	}
    }

}

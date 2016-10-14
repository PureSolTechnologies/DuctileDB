package com.puresoltechnologies.ductiledb.core.cli;

import java.io.IOException;
import java.io.PrintStream;

import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnTypeDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.ColumnDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.core.tables.dml.DataManipulationLanguage;
import com.puresoltechnologies.ductiledb.core.tables.dml.Select;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRow;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;

public class DuctileDBConsoleOutput {

    private final PrintStream printStream;

    public DuctileDBConsoleOutput(PrintStream printStream) {
	super();
	this.printStream = printStream;
    }

    public void printTableContent(TableStoreImpl tableStore, String namespace, String table) throws IOException {
	StringBuilder builder = new StringBuilder();
	TableDefinition tableDefinition = tableStore.getTableDefinition(namespace, table);
	builder.append("---------------------------------------------------------\n");
	builder.append("TABLE: " + tableDefinition.getNamespace() + "." + tableDefinition.getName() + "\n");
	for (ColumnDefinition<?> columnDefinition : tableDefinition.getColumnDefinitions()) {
	    builder.append(columnDefinition.getName() + "\t");
	}
	builder.append("\n");
	builder.append("---------------------------------------------------------\n");
	DataManipulationLanguage dml = tableStore.getDataManipulationLanguage();
	Select select = dml.createSelect(namespace, table);
	try (TableRowIterable tableRows = select.execute()) {
	    for (TableRow tableRow : tableRows) {
		for (ColumnDefinition<?> columnDefinition : tableDefinition.getColumnDefinitions()) {
		    ColumnTypeDefinition<?> type = columnDefinition.getType();
		    byte[] value = tableRow.getBytes(columnDefinition.getName());
		    builder.append(type.fromBytes(value) + "\t");
		}
		builder.append("\n");
	    }
	    printStream.println(builder.toString());
	}
    }

}

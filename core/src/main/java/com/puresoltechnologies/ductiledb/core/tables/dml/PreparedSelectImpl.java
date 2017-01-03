package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.puresoltechnologies.ductiledb.bigtable.ResultScanner;
import com.puresoltechnologies.ductiledb.bigtable.Scan;
import com.puresoltechnologies.ductiledb.bigtable.TableEngineImpl;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;

public class PreparedSelectImpl extends AbstractPreparedWhereSelectionStatement implements PreparedSelect {

    private final Map<String, String> columnSelection = new HashMap<>();

    public PreparedSelectImpl(TableStoreImpl tableStore, TableDefinition tableDefinition) {
	super(tableStore, tableDefinition);
    }

    @Override
    public PreparedSelect selectColumn(String column, String alias) {
	columnSelection.put(column, alias);
	return this;
    }

    @Override
    public TableRowIterable execute(Map<Integer, Comparable<?>> placeholderValue) {
	TableEngineImpl tableEngine = getTableEngine();
	ResultScanner scanner = tableEngine.getScanner(new Scan());
	Set<WhereClause<?>> selections = getSelections(placeholderValue);
	return new TableRowIterableImpl<>(scanner,
		result -> TableRowCreator.create(getTableDefinition(), result, columnSelection), row -> {
		    for (WhereClause<?> selection : selections) {
			if (!selection.matches(row)) {
			    return false;
			}
		    }
		    return true;
		});
    }

}

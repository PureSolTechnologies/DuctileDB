package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.puresoltechnologies.ductiledb.core.tables.TableStore;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.storage.engine.ResultScanner;
import com.puresoltechnologies.ductiledb.storage.engine.Scan;
import com.puresoltechnologies.ductiledb.storage.engine.TableEngineImpl;

public class PreparedSelectImpl extends AbstractPreparedWhereSelectionStatement implements PreparedSelect {

    private final Map<String, String> columnSelection = new HashMap<>();

    public PreparedSelectImpl(TableDefinition tableDefinition) {
	super(tableDefinition);
    }

    @Override
    public PreparedSelect selectColumn(String column, String alias) {
	columnSelection.put(column, alias);
	return this;
    }

    @Override
    public TableRowIterable execute(TableStore tableStore, Map<Integer, Comparable<?>> placeholderValue) {
	TableEngineImpl tableEngine = getTableEngine(tableStore);
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

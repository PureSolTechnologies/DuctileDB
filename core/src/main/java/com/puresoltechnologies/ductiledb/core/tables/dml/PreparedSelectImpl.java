package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.Map;
import java.util.Set;

import com.puresoltechnologies.ductiledb.core.tables.TableStore;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.storage.engine.ResultScanner;
import com.puresoltechnologies.ductiledb.storage.engine.Scan;
import com.puresoltechnologies.ductiledb.storage.engine.TableEngineImpl;

public class PreparedSelectImpl extends AbstractPreparedWhereSelectionStatement implements PreparedSelect {

    public PreparedSelectImpl(TableDefinition tableDefinition) {
	super(tableDefinition);
    }

    @Override
    public TableRowIterable execute(TableStore tableStore, Map<Integer, Comparable<?>> placeholderValue) {
	TableEngineImpl tableEngine = getTableEngine(tableStore);
	ResultScanner scanner = tableEngine.getScanner(new Scan());
	Set<WhereClause<?>> selections = getSelections(placeholderValue);
	return new TableRowIterableImpl<>(scanner, result -> TableRowCreator.create(getTableDefinition(), result),
		row -> {
		    for (WhereClause<?> selection : selections) {
			if (!selection.matches(row)) {
			    return false;
			}
		    }
		    return true;
		});
    }

}

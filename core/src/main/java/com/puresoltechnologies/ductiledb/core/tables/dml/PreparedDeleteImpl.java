package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.Map;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStore;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.storage.engine.Delete;
import com.puresoltechnologies.ductiledb.storage.engine.TableEngineImpl;

public class PreparedDeleteImpl extends AbstractPreparedWhereSelectionStatement implements PreparedDelete {

    public PreparedDeleteImpl(TableDefinition tableDefinition) {
	super(tableDefinition);
    }

    @Override
    public TableRowIterable execute(TableStore tableStore, Map<Integer, Comparable<?>> placeholderValues)
	    throws ExecutionException {
	DataManipulationLanguage dml = tableStore.getDataManipulationLanguage();
	PreparedSelect select = dml.prepareSelect(getNamespace(), getTableName());
	select.addWhereSelections(getSelections(placeholderValues));
	TableRowIterable rows = select.bind().execute(tableStore);
	TableEngineImpl tableEngine = getTableEngine(tableStore);
	for (TableRow row : rows) {
	    tableEngine.delete(new Delete(row.getRowKey()));
	}
	return null;
    }

}

package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.Map;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.engine.Delete;
import com.puresoltechnologies.ductiledb.engine.TableEngineImpl;

public class PreparedDeleteImpl extends AbstractPreparedWhereSelectionStatement implements PreparedDelete {

    public PreparedDeleteImpl(TableStoreImpl tableStore, TableDefinition tableDefinition) {
	super(tableStore, tableDefinition);
    }

    @Override
    public TableRowIterable execute(Map<Integer, Comparable<?>> placeholderValues) throws ExecutionException {
	TableStoreImpl tableStore = getTableStore();
	DataManipulationLanguage dml = tableStore.getDataManipulationLanguage();
	PreparedSelect select = dml.prepareSelect(getNamespace(), getTableName());
	select.addWhereSelections(getSelections(placeholderValues));
	TableRowIterable rows = select.bind().execute();
	TableEngineImpl tableEngine = getTableEngine();
	for (TableRow row : rows) {
	    tableEngine.delete(new Delete(row.getRowKey()));
	}
	return null;
    }

}

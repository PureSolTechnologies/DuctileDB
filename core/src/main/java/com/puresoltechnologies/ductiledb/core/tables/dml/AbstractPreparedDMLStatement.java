package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.puresoltechnologies.ductiledb.core.tables.AbstractPreparedStatement;
import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStore;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.storage.engine.NamespaceEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.TableEngineImpl;

public abstract class AbstractPreparedDMLStatement extends AbstractPreparedStatement implements PreparedDMLStatement {

    private final Map<Integer, Placeholder> placeholders = new HashMap<>();

    private final TableDefinition tableDefinition;

    public AbstractPreparedDMLStatement(TableDefinition tableDefinition) {
	super();
	if (tableDefinition == null) {
	    throw new IllegalArgumentException("Table definition must not be null.");
	}
	this.tableDefinition = tableDefinition;
    }

    public final TableDefinition getTableDefinition() {
	return tableDefinition;
    }

    public final String getNamespace() {
	return tableDefinition.getNamespace();
    }

    public final String getTableName() {
	return tableDefinition.getName();
    }

    protected final TableEngineImpl getTableEngine(TableStore tableStore) {
	NamespaceEngineImpl namespaceEngine = ((TableStoreImpl) tableStore).getStorageEngine()
		.getNamespaceEngine(getNamespace());
	TableEngineImpl tableEngine = namespaceEngine.getTableEngine(getTableName());
	return tableEngine;
    }

    @Override
    public final PreparedDMLStatement addPlaceholder(Placeholder placeholder) {
	placeholders.put(placeholder.getIndex(), placeholder);
	return this;
    }

    public final Map<Integer, Placeholder> getPlaceholders() {
	return placeholders;
    }

    public final Placeholder getPlaceholder(int index) {
	return placeholders.get(index);
    }

    public final int getPlaceholderIndex(String columnName) {
	for (Entry<Integer, Placeholder> placeholderEntry : placeholders.entrySet()) {
	    if (columnName.equals(placeholderEntry.getValue().getColumn())) {
		return placeholderEntry.getKey();
	    }
	}
	return -1;
    }

    @Override
    public abstract TableRowIterable execute(TableStore tableStore, Map<Integer, Comparable<?>> placeholderValue)
	    throws ExecutionException;

}

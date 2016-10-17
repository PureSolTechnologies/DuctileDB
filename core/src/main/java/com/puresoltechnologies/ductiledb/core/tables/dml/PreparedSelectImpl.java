package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.storage.engine.NamespaceEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.Result;
import com.puresoltechnologies.ductiledb.storage.engine.ResultScanner;
import com.puresoltechnologies.ductiledb.storage.engine.Scan;
import com.puresoltechnologies.ductiledb.storage.engine.TableEngineImpl;

public class PreparedSelectImpl extends AbstractPreparedWhereSelectionStatement implements PreparedSelect {

    private final TableStoreImpl tableStore;
    private final String namespace;
    private final String table;
    private final NamespaceEngineImpl namespaceEngine;
    private final TableEngineImpl tableEngine;

    public PreparedSelectImpl(TableStoreImpl tableStore, String namespace, String table) {
	super(tableStore.getTableDefinition(namespace, table));
	this.tableStore = tableStore;
	this.namespace = namespace;
	this.table = table;
	this.namespaceEngine = tableStore.getStorageEngine().getNamespaceEngine(namespace);
	this.tableEngine = namespaceEngine.getTableEngine(table);
    }

    @Override
    public TableRowIterable execute(Map<String, Object> valueSpecifications) {
	ResultScanner scanner = tableEngine.getScanner(new Scan());
	return new TableRowIterable() {

	    @Override
	    public Iterator<TableRow> iterator() {
		return new Iterator<TableRow>() {

		    @Override
		    public boolean hasNext() {
			return scanner.hasNext();
		    }

		    @Override
		    public TableRow next() {
			Result next = scanner.next();

			return TableRowCreator.create(getTableDefinition(), next);
		    }
		};
	    }

	    @Override
	    public void close() throws IOException {
		scanner.close();
	    }
	};
    }

}

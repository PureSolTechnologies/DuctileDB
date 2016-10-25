package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.puresoltechnologies.ductiledb.core.tables.TableStore;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.storage.engine.NamespaceEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.Result;
import com.puresoltechnologies.ductiledb.storage.engine.ResultScanner;
import com.puresoltechnologies.ductiledb.storage.engine.Scan;
import com.puresoltechnologies.ductiledb.storage.engine.TableEngineImpl;

public class PreparedSelectImpl extends AbstractPreparedWhereSelectionStatement implements PreparedSelect {

    private final String namespace;
    private final String table;

    public PreparedSelectImpl(TableDefinition tableDefinition) {
	super(tableDefinition);
	this.namespace = tableDefinition.getNamespace();
	this.table = tableDefinition.getName();
    }

    @Override
    public TableRowIterable execute(TableStore tableStore, Map<Integer, Object> placeholderValue) {
	NamespaceEngineImpl namespaceEngine = ((TableStoreImpl) tableStore).getStorageEngine()
		.getNamespaceEngine(namespace);
	TableEngineImpl tableEngine = namespaceEngine.getTableEngine(table);
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

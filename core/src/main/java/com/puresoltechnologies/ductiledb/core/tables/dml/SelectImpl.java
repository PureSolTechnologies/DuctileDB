package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.io.IOException;
import java.util.Iterator;

import com.puresoltechnologies.ductiledb.api.tables.dml.Select;
import com.puresoltechnologies.ductiledb.api.tables.dml.TableRow;
import com.puresoltechnologies.ductiledb.api.tables.dml.TableRowIterable;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.storage.engine.NamespaceEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.Result;
import com.puresoltechnologies.ductiledb.storage.engine.ResultScanner;
import com.puresoltechnologies.ductiledb.storage.engine.Scan;
import com.puresoltechnologies.ductiledb.storage.engine.TableEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class SelectImpl implements Select {

    private final TableStoreImpl tableStore;
    private final String namespace;
    private final String table;
    private final NamespaceEngineImpl namespaceEngine;
    private final TableEngineImpl tableEngine;

    public SelectImpl(TableStoreImpl tableStore, String namespace, String table) {
	this.tableStore = tableStore;
	this.namespace = namespace;
	this.table = table;
	this.namespaceEngine = tableStore.getStorageEngine().getNamespaceEngine(namespace);
	this.tableEngine = namespaceEngine.getTableEngine(table);
    }

    @Override
    public TableRowIterable execute() {
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
			byte[] rowKey = next.getRowKey();

			for (byte[] family : next.getFamilies()) {
			    String familyName = Bytes.toString(family);

			}
			return new TableRow();
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

package com.puresoltechnologies.ductiledb.core.tables.ddl;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import com.puresoltechnologies.commons.misc.io.CloseableIterable;
import com.puresoltechnologies.ductiledb.api.tables.ddl.CreateIndex;
import com.puresoltechnologies.ductiledb.api.tables.ddl.CreateNamespace;
import com.puresoltechnologies.ductiledb.api.tables.ddl.CreateTable;
import com.puresoltechnologies.ductiledb.api.tables.ddl.DataDefinitionLanguage;
import com.puresoltechnologies.ductiledb.api.tables.ddl.DropIndex;
import com.puresoltechnologies.ductiledb.api.tables.ddl.DropNamespace;
import com.puresoltechnologies.ductiledb.api.tables.ddl.DropTable;
import com.puresoltechnologies.ductiledb.api.tables.ddl.NamespaceDefinition;
import com.puresoltechnologies.ductiledb.api.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.api.tables.dml.DataManipulationLanguage;
import com.puresoltechnologies.ductiledb.api.tables.dml.Select;
import com.puresoltechnologies.ductiledb.api.tables.dml.TableRow;
import com.puresoltechnologies.ductiledb.api.tables.dml.TableRowIterable;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.schema.DatabaseTable;
import com.puresoltechnologies.ductiledb.core.tables.schema.TableStoreSchema;

public class DataDefinitionLanguageImpl implements DataDefinitionLanguage {

    private final TableStoreImpl tableStore;
    private final File directory;

    public DataDefinitionLanguageImpl(TableStoreImpl tableStore, File directory) {
	this.tableStore = tableStore;
	this.directory = directory;
    }

    @Override
    public CreateNamespace createCreateNamespace(String namespace) {
	return new CreateNamespaceImpl(tableStore, namespace);
    }

    @Override
    public DropNamespace createDropNamespace(String namespace) {
	return new DropNamespaceImpl(tableStore, namespace);
    }

    @Override
    public NamespaceDefinition getNamespace(String namespace) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public CloseableIterable<NamespaceDefinition> getNamespaces() {
	DataManipulationLanguage dml = tableStore.getDataManipulationLanguage();
	Select select = dml.createSelect(TableStoreSchema.SYSTEM_NAMESPACE_NAME, DatabaseTable.NAMESPACES.getName());
	return new CloseableIterable<NamespaceDefinition>() {

	    private final TableRowIterable results = select.execute();
	    private final Iterator<TableRow> iterator = results.iterator();

	    @Override
	    public void close() throws IOException {
		results.close();
	    }

	    @Override
	    public Iterator<NamespaceDefinition> iterator() {
		return new Iterator<NamespaceDefinition>() {

		    @Override
		    public boolean hasNext() {
			return iterator.hasNext();
		    }

		    @Override
		    public NamespaceDefinition next() {
			TableRow result = iterator.next();
			return new NamespaceDefinitionImpl(result.getString("name"));
		    }
		};
	    }
	};
    }

    @Override
    public CreateTable createCreateTable(String namespace, String table) {
	return new CreateTableImpl(tableStore, namespace, table);
    }

    @Override
    public DropTable createDropTable(String namespace, String table) {
	return new DropTableImpl(tableStore, namespace, table);
    }

    @Override
    public TableDefinition getTable(String namespace, String table) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public CreateIndex createCreateIndex(String namespace, String table, String index) {
	return new CreateIndexImpl(tableStore, namespace, table, index);
    }

    @Override
    public DropIndex createDropIndex(String namespace, String table, String index) {
	return new DropIndexImpl(tableStore, namespace, table, index);
    }
}

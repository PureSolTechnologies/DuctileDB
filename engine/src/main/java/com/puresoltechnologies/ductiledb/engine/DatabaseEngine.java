package com.puresoltechnologies.ductiledb.engine;

import java.io.Closeable;

import com.puresoltechnologies.ductiledb.bigtable.TableDescriptor;
import com.puresoltechnologies.ductiledb.bigtable.TableEngine;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaManager;

/**
 * This class is the database engine class. It supports a schema, and multiple
 * big table storages organized in column families. It is using the
 * {@link TableEngine} to store the separate tables.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DatabaseEngine extends Closeable {

    public boolean isClosed();

    public String getStoreName();

    public SchemaManager getSchemaManager();

    public TableEngine getTable(TableDescriptor tableDescriptor);

    public TableEngine getTable(String namespaceName, String tableName);

}

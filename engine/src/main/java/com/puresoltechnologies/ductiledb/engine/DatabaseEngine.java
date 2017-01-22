package com.puresoltechnologies.ductiledb.engine;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

import com.puresoltechnologies.ductiledb.bigtable.BigTable;

/**
 * This class is the database engine class. It supports a schema, and multiple
 * big table storages organized in column families. It is using the
 * {@link BigTable} to store the separate tables.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DatabaseEngine extends Closeable {

    public boolean isClosed();

    public String getStoreName();

    public Set<String> getNamespaces();

    public Namespace addNamespace(String namespace) throws IOException;

    public Namespace getNamespace(String namespace);

    public boolean hasNamespace(String namespaceName);

    public void dropNamespace(String namespaceName) throws IOException;

}

package com.puresoltechnologies.ductiledb.engine;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Set;

import com.puresoltechnologies.ductiledb.bigtable.BigTable;
import com.puresoltechnologies.ductiledb.bigtable.BigTableConfiguration;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This is the interface for namespace engines.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface Namespace extends Closeable {

    public static Namespace create(Storage storage, NamespaceDescriptor namespaceDescriptor,
	    BigTableConfiguration configuration) throws IOException {
	return new NamespaceImpl(storage, namespaceDescriptor, configuration);
    }

    public static Namespace reopen(Storage storage, File directory) throws IOException {
	return new NamespaceImpl(storage, directory);
    }

    public String getName();

    /**
     * This method returns the descriptor of the namespace.
     * 
     * @return
     */
    public NamespaceDescriptor getDescriptor();

    public Set<String> getTables();

    BigTable addTable(String name, String description) throws IOException;

    BigTable getTable(String name);

    boolean hasTable(String name);

    void dropTable(String name);

}

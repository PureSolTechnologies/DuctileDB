package com.puresoltechnologies.ductiledb.engine;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import com.puresoltechnologies.ductiledb.bigtable.BigTableEngineConfiguration;
import com.puresoltechnologies.ductiledb.bigtable.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This is the interface for namespace engines.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface NamespaceEngine extends Closeable {

    public static NamespaceEngine create(Storage storage, NamespaceDescriptor namespaceDescriptor,
	    BigTableEngineConfiguration configuration) throws IOException {
	return new NamespaceEngineImpl(storage, namespaceDescriptor, configuration);
    }

    public static NamespaceEngine reopen(Storage storage, File directory) throws IOException {
	return new NamespaceEngineImpl(storage, directory);
    }

    /**
     * This method returns the descriptor of the namespace.
     * 
     * @return
     */
    public NamespaceDescriptor getDescriptor();

}

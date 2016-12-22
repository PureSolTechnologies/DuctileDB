package com.puresoltechnologies.ductiledb.engine;

import java.io.Closeable;

import com.puresoltechnologies.ductiledb.engine.schema.NamespaceDescriptor;

/**
 * This is the interface for namespace engines.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface NamespaceEngine extends Closeable {

    /**
     * This method returns the descriptor of the namespace.
     * 
     * @return
     */
    public NamespaceDescriptor getDescriptor();

}

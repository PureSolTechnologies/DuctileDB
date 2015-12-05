package com.puresoltechnologies.ductiledb.tinkerpop;

import java.io.IOException;

import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;

public class DuctileTransaction extends AbstractThreadLocalTransaction {

    private final DuctileGraph graph;

    public DuctileTransaction(DuctileGraph graph) {
	super(graph);
	this.graph = graph;
    }

    @Override
    protected void doOpen() {
	// Nothing to do, because of implicit transactions...
    }

    @Override
    protected void doCommit() throws TransactionException {
	try {
	    graph.getBaseGraph().commit();
	} catch (IOException e) {
	    throw new TransactionException("Could not commit transaction.", e);
	}
    }

    @Override
    protected void doRollback() throws TransactionException {
	try {
	    graph.getBaseGraph().rollback();
	} catch (IOException e) {
	    throw new TransactionException("Could not rollback transaction.", e);
	}
    }

    @Override
    public boolean isOpen() {
	return true;
    }

}

package com.puresoltechnologies.ductiledb.tinkerpop;

import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;

public class DuctileTransaction extends AbstractThreadLocalTransaction {

    private final DuctileGraph graph;
    private ThreadLocal<Boolean> open = ThreadLocal.withInitial(() -> false);

    public DuctileTransaction(DuctileGraph graph) {
	super(graph);
	this.graph = graph;
    }

    @Override
    protected void doOpen() {
	open.set(true);
    }

    @Override
    protected void doCommit() throws TransactionException {
	try {
	    graph.getBaseGraph().commit();
	} catch (Exception ex) {
	    throw new TransactionException(ex);
	} finally {
	    open.set(false);
	}
    }

    @Override
    protected void doRollback() throws TransactionException {
	try {
	    graph.getBaseGraph().rollback();
	} catch (Exception e) {
	    throw new TransactionException(e);
	} finally {
	    open.set(false);
	}
    }

    @Override
    public boolean isOpen() {
	return open.get();
    }

}

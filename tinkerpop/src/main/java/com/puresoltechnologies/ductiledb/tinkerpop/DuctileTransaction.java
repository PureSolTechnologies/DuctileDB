package com.puresoltechnologies.ductiledb.tinkerpop;

import java.io.IOException;

import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;

import com.puresoltechnologies.ductiledb.api.tx.DuctileDBTransaction;

public class DuctileTransaction extends AbstractThreadLocalTransaction {

    private final ThreadLocal<DuctileDBTransaction> threadLocalTx = ThreadLocal.withInitial(() -> null);

    private final DuctileGraph graph;

    public DuctileTransaction(DuctileGraph graph) {
	super(graph);
	this.graph = graph;
    }

    @Override
    protected void doOpen() {
	threadLocalTx.set(graph.getBaseGraph().createTransaction());
    }

    @Override
    protected void doCommit() throws TransactionException {
	try {
	    threadLocalTx.get().commit();
	} catch (Exception ex) {
	    throw new TransactionException(ex);
	} finally {
	    try {
		threadLocalTx.get().close();
	    } catch (IOException e) {
		// intentionally left empty...
	    }
	    threadLocalTx.remove();
	}
    }

    @Override
    protected void doRollback() throws TransactionException {
	try {
	    threadLocalTx.get().rollback();
	} catch (Exception e) {
	    throw new TransactionException(e);
	} finally {
	    try {
		threadLocalTx.get().close();
	    } catch (IOException e) {
		// intentionally left empty...
	    }
	    threadLocalTx.remove();
	}
    }

    @Override
    public boolean isOpen() {
	return (threadLocalTx.get() != null);
    }

}

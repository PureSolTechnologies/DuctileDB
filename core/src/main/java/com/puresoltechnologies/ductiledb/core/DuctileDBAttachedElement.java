package com.puresoltechnologies.ductiledb.core;

import com.puresoltechnologies.ductiledb.core.tx.DuctileDBTransactionImpl;

public abstract class DuctileDBAttachedElement extends AbstractDuctileDBElement {

    private final DuctileDBGraphImpl graph;
    private final boolean threadLocalTransaction;

    public DuctileDBAttachedElement(DuctileDBGraphImpl graph, DuctileDBTransactionImpl transaction, long id) {
	super(transaction, id);
	this.graph = graph;
	this.threadLocalTransaction = (graph.getCurrentTransaction() == transaction);
    }

    @Override
    public DuctileDBTransactionImpl getTransaction() {
	if (threadLocalTransaction) {
	    return (DuctileDBTransactionImpl) graph.getCurrentTransaction();
	} else {
	    return super.getTransaction();
	}
    }

}

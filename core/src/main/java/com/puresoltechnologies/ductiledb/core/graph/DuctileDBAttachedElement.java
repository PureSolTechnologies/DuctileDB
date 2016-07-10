package com.puresoltechnologies.ductiledb.core.graph;

import com.puresoltechnologies.ductiledb.core.graph.tx.DuctileDBTransactionImpl;

public abstract class DuctileDBAttachedElement extends AbstractDuctileDBElement {

    public DuctileDBAttachedElement(DuctileDBTransactionImpl transaction, long id) {
	super(transaction, id);
    }

}

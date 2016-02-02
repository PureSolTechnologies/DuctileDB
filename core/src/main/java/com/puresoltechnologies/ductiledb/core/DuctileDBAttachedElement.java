package com.puresoltechnologies.ductiledb.core;

import com.puresoltechnologies.ductiledb.core.tx.DuctileDBTransactionImpl;

public abstract class DuctileDBAttachedElement extends AbstractDuctileDBElement {

    public DuctileDBAttachedElement(DuctileDBTransactionImpl transaction, long id) {
	super(transaction, id);
    }

}

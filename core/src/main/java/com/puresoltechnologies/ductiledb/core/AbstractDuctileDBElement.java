package com.puresoltechnologies.ductiledb.core;

import com.puresoltechnologies.ductiledb.api.DuctileDBElement;
import com.puresoltechnologies.ductiledb.api.tx.TransactionType;
import com.puresoltechnologies.ductiledb.core.tx.DuctileDBTransactionImpl;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;

public abstract class AbstractDuctileDBElement implements DuctileDBElement {

    private DuctileDBTransactionImpl transaction;
    private final long id;

    protected AbstractDuctileDBElement(DuctileDBTransactionImpl transaction, long id) {
	super();
	if (transaction == null) {
	    throw new IllegalArgumentException("Transaction must not be null.");
	}
	if (id <= 0) {
	    throw new IllegalArgumentException("Id must be a positive number.");
	}
	this.transaction = transaction;
	this.id = id;
    }

    @Override
    public DuctileDBTransactionImpl getTransaction() {
	if ((!transaction.isOpen()) && (transaction.getTransactionType() == TransactionType.THREAD_LOCAL)) {
	    transaction = (DuctileDBTransactionImpl) transaction.getGraph().getCurrentTransaction();
	}
	return transaction;
    }

    @Override
    public final long getId() {
	return id;
    }

    protected final String getPropertiesString() {
	StringBuilder builder = new StringBuilder("{");
	boolean first = true;
	for (String key : getPropertyKeys()) {
	    if (first) {
		first = false;
	    } else {
		builder.append(", ");
	    }
	    Object value = getProperty(key);
	    builder.append(key);
	    builder.append('=');
	    builder.append(value);
	}
	builder.append('}');
	return builder.toString();
    }

    @Override
    public AbstractDuctileDBElement clone() {
	try {
	    AbstractDuctileDBElement cloned = (AbstractDuctileDBElement) super.clone();
	    ElementUtils.setFinalField(cloned, AbstractDuctileDBElement.class, "id", id);
	    ElementUtils.setFinalField(cloned, AbstractDuctileDBElement.class, "transaction", transaction);
	    return cloned;
	} catch (CloneNotSupportedException e) {
	    throw new RuntimeException(e);
	}
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + (int) (id ^ (id >>> 32));
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	AbstractDuctileDBElement other = (AbstractDuctileDBElement) obj;
	if (id != other.id)
	    return false;
	return true;
    }

}

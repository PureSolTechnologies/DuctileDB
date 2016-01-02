package com.puresoltechnologies.ductiledb.core;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.core.tx.DuctileDBTransactionImpl;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;

public abstract class DuctileDBAttachedElement extends AbstractDuctileDBElement {

    private final DuctileDBGraphImpl graph;
    private final long id;

    public DuctileDBAttachedElement(DuctileDBGraphImpl graph, long id) {
	super();
	if (graph == null) {
	    throw new IllegalArgumentException("Graph must not be null.");
	}
	if (id <= 0) {
	    throw new IllegalArgumentException("Id must be a positive number.");
	}
	this.graph = graph;
	this.id = id;
    }

    public DuctileDBTransactionImpl getCurrentTransaction() {
	return (DuctileDBTransactionImpl) graph.getCurrentTransaction();
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((graph == null) ? 0 : graph.hashCode());
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
	DuctileDBAttachedElement other = (DuctileDBAttachedElement) obj;
	if (graph == null) {
	    if (other.graph != null)
		return false;
	} else if (!graph.equals(other.graph))
	    return false;
	if (id != other.id)
	    return false;
	return true;
    }

    @Override
    public final DuctileDBGraph getGraph() {
	return graph;
    }

    @Override
    public final long getId() {
	return id;
    }

    @Override
    public DuctileDBAttachedElement clone() {
	DuctileDBAttachedElement cloned = (DuctileDBAttachedElement) super.clone();
	ElementUtils.setFinalField(cloned, DuctileDBAttachedElement.class, "graph", graph);
	ElementUtils.setFinalField(cloned, DuctileDBAttachedElement.class, "id", id);
	return cloned;
    }
}

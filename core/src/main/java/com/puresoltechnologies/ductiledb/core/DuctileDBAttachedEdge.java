package com.puresoltechnologies.ductiledb.core;

import java.util.Set;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.tx.DuctileDBTransactionImpl;

public class DuctileDBAttachedEdge extends DuctileDBAttachedElement implements DuctileDBEdge {

    public DuctileDBAttachedEdge(DuctileDBGraphImpl graph, DuctileDBTransactionImpl transaction, long id) {
	super(graph, transaction, id);
    }

    @Override
    public DuctileDBVertex getStartVertex() {
	return getTransaction().getEdgeStartVertex(getId());
    }

    @Override
    public DuctileDBVertex getTargetVertex() {
	return getTransaction().getEdgeTargetVertex(getId());
    }

    @Override
    public String getType() {
	return getTransaction().getEdgeType(getId());
    }

    @Override
    public Set<String> getPropertyKeys() {
	return getTransaction().getEdgePropertyKeys(getId());
    }

    @Override
    public <T> T getProperty(String key) {
	return getTransaction().getEdgeProperty(getId(), key);
    }

    @Override
    public <T> void setProperty(String key, T value) {
	getTransaction().setProperty(this, key, value);
    }

    @Override
    public void removeProperty(String key) {
	getTransaction().removeProperty(this, key);
    }

    @Override
    public void remove() {
	getTransaction().removeEdge(this);
    }

    @Override
    public String toString() {
	return getClass().getSimpleName() + " " + getId() + " (" + getStartVertex().getId() + "->"
		+ getTargetVertex().getId() + "): type=" + getType() + "; properties=" + getPropertiesString();
    }

    @Override
    public DuctileDBAttachedEdge clone() {
	return (DuctileDBAttachedEdge) super.clone();
    }
}

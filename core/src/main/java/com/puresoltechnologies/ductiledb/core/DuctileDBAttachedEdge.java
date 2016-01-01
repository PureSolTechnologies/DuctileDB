package com.puresoltechnologies.ductiledb.core;

import java.util.Set;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;

public class DuctileDBAttachedEdge extends DuctileDBAttachedElement implements DuctileDBEdge {

    public DuctileDBAttachedEdge(DuctileDBGraphImpl graph, long id) {
	super(graph, id);
    }

    @Override
    public DuctileDBVertex getStartVertex() {
	return getCurrentTransaction().getEdgeStartVertex(getId());
    }

    @Override
    public DuctileDBVertex getTargetVertex() {
	return getCurrentTransaction().getEdgeTargetVertex(getId());
    }

    @Override
    public String getType() {
	return getCurrentTransaction().getEdgeType(getId());
    }

    @Override
    public Set<String> getPropertyKeys() {
	return getCurrentTransaction().getEdgePropertyKeys(getId());
    }

    @Override
    public <T> T getProperty(String key) {
	return getCurrentTransaction().getEdgeProperty(getId(), key);
    }

    @Override
    public <T> void setProperty(String key, T value) {
	getCurrentTransaction().setProperty(this, key, value);
    }

    @Override
    public void removeProperty(String key) {
	getCurrentTransaction().removeProperty(this, key);
    }

    @Override
    public void remove() {
	getCurrentTransaction().removeEdge(this);
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

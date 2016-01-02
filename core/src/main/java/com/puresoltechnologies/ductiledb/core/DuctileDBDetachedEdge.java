package com.puresoltechnologies.ductiledb.core;

import java.util.Map;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBException;
import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;

public class DuctileDBDetachedEdge extends DuctileDBDetachedElement implements DuctileDBEdge {

    private final String type;
    private final long startVertexId;
    private final long targetVertexId;
    private DuctileDBVertex startVertex = null;
    private DuctileDBVertex targetVertex = null;

    public DuctileDBDetachedEdge(DuctileDBGraphImpl graph, long id, String type, long startVertexId,
	    long targetVertexId, Map<String, Object> properties) {
	super(graph, id, properties);
	this.type = type;
	this.startVertexId = startVertexId;
	this.targetVertexId = targetVertexId;
    }

    public DuctileDBDetachedEdge(DuctileDBGraphImpl graph, long id, String type, DuctileDBVertex startVertex,
	    long targetVertexId, Map<String, Object> properties) {
	super(graph, id, properties);
	this.type = type;
	this.startVertex = startVertex;
	this.startVertexId = startVertex.getId();
	this.targetVertexId = targetVertexId;
    }

    public DuctileDBDetachedEdge(DuctileDBGraphImpl graph, long id, String type, long startVertexId,
	    DuctileDBVertex targetVertex, Map<String, Object> properties) {
	super(graph, id, properties);
	this.type = type;
	this.startVertexId = startVertexId;
	this.targetVertex = targetVertex;
	this.targetVertexId = targetVertex.getId();
    }

    public DuctileDBDetachedEdge(DuctileDBGraphImpl graph, long id, String type, DuctileDBVertex startVertex,
	    DuctileDBVertex targetVertex, Map<String, Object> properties) {
	super(graph, id, properties);
	this.type = type;
	this.startVertexId = startVertex.getId();
	this.targetVertexId = targetVertex.getId();
	this.startVertex = startVertex;
	this.targetVertex = targetVertex;
    }

    private void throwDetachedException() {
	throw new DuctileDBException("This edge is detached from graph and cannot be altered.");
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = super.hashCode();
	result = prime * result + ((type == null) ? 0 : type.hashCode());
	result = prime * result + (int) (startVertexId ^ (startVertexId >>> 32));
	result = prime * result + (int) (targetVertexId ^ (targetVertexId >>> 32));
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (!super.equals(obj))
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	DuctileDBDetachedEdge other = (DuctileDBDetachedEdge) obj;
	if (type == null) {
	    if (other.type != null)
		return false;
	} else if (!type.equals(other.type))
	    return false;
	if (startVertexId != other.startVertexId)
	    return false;
	if (targetVertexId != other.targetVertexId)
	    return false;
	return true;
    }

    @Override
    public DuctileDBVertex getStartVertex() {
	if (startVertex == null) {
	    startVertex = getGraph().getVertex(startVertexId);
	}
	return startVertex;
    }

    public long getStartVertexId() {
	return startVertexId;
    }

    @Override
    public DuctileDBVertex getTargetVertex() {
	if (targetVertex == null) {
	    targetVertex = getGraph().getVertex(targetVertexId);
	}
	return targetVertex;
    }

    public long getTargetVertexId() {
	return targetVertexId;
    }

    @Override
    public String getType() {
	return type;
    }

    @Override
    protected <T> void setProperty(DuctileDBGraph graph, String key, T value) {
	throwDetachedException();
    }

    @Override
    protected void removeProperty(DuctileDBGraph graph, String key) {
	throwDetachedException();
    }

    @Override
    public void remove() {
	throwDetachedException();
    }

    @Override
    public String toString() {
	return getClass().getSimpleName() + " " + getId() + " (" + startVertexId + "->" + targetVertexId + "): type="
		+ type + "; properties=" + getPropertiesString();
    }

    @Override
    public DuctileDBDetachedEdge clone() {
	DuctileDBDetachedEdge cloned = (DuctileDBDetachedEdge) super.clone();
	ElementUtils.setFinalField(cloned, DuctileDBDetachedEdge.class, "type", type);
	ElementUtils.setFinalField(cloned, DuctileDBDetachedEdge.class, "startVertexId", startVertexId);
	ElementUtils.setFinalField(cloned, DuctileDBDetachedEdge.class, "targetVertexId", targetVertexId);
	ElementUtils.setFinalField(cloned, DuctileDBDetachedEdge.class, "startVertex", null);
	ElementUtils.setFinalField(cloned, DuctileDBDetachedEdge.class, "targetVertex", null);
	return cloned;
    }
}

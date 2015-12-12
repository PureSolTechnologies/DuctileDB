package com.puresoltechnologies.ductiledb.core;

import java.util.Map;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;

public class DuctileDBEdgeImpl extends DuctileDBElementImpl implements DuctileDBEdge {

    private final String label;
    private final long startVertexId;
    private final long targetVertexId;
    private DuctileDBVertex startVertex = null;
    private DuctileDBVertex targetVertex = null;

    public DuctileDBEdgeImpl(DuctileDBGraphImpl graph, long id, String label, long startVertexId, long targetVertexId,
	    Map<String, Object> properties) {
	super(graph, id, properties);
	this.label = label;
	this.startVertexId = startVertexId;
	this.targetVertexId = targetVertexId;
    }

    public DuctileDBEdgeImpl(DuctileDBGraphImpl graph, long id, String label, DuctileDBVertex startVertex,
	    long targetVertexId, Map<String, Object> properties) {
	super(graph, id, properties);
	this.label = label;
	this.startVertex = startVertex;
	this.startVertexId = startVertex.getId();
	this.targetVertexId = targetVertexId;
    }

    public DuctileDBEdgeImpl(DuctileDBGraphImpl graph, long id, String label, long startVertexId,
	    DuctileDBVertex targetVertex, Map<String, Object> properties) {
	super(graph, id, properties);
	this.label = label;
	this.startVertexId = startVertexId;
	this.targetVertex = targetVertex;
	this.targetVertexId = targetVertex.getId();
    }

    public DuctileDBEdgeImpl(DuctileDBGraphImpl graph, long id, String label, DuctileDBVertex startVertex,
	    DuctileDBVertex targetVertex, Map<String, Object> properties) {
	super(graph, id, properties);
	this.label = label;
	this.startVertexId = startVertex.getId();
	this.targetVertexId = targetVertex.getId();
	this.startVertex = startVertex;
	this.targetVertex = targetVertex;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = super.hashCode();
	result = prime * result + ((label == null) ? 0 : label.hashCode());
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
	DuctileDBEdgeImpl other = (DuctileDBEdgeImpl) obj;
	if (label == null) {
	    if (other.label != null)
		return false;
	} else if (!label.equals(other.label))
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
    public DuctileDBVertex getVertex(EdgeDirection direction) throws IllegalArgumentException {
	switch (direction) {
	case OUT:
	    return getStartVertex();
	case IN:
	    return getTargetVertex();
	default:
	    throw new IllegalArgumentException("Direction '" + direction + "' is not supported.");
	}
    }

    @Override
    public String getLabel() {
	return label;
    }

    @Override
    protected void setProperty(DuctileDBGraph graph, String key, Object value) {
	graph.setProperty(this, key, value);
    }

    @Override
    protected void removeProperty(DuctileDBGraph graph, String key) {
	graph.removeProperty(this, key);
    }

    @Override
    public void remove() {
	getGraph().removeEdge(this);
    }

    @Override
    public String toString() {
	return "edge " + getId() + " (" + startVertexId + "->" + targetVertexId + "): label=" + label + "; properties="
		+ getPropertiesString();
    }

    @Override
    public DuctileDBEdgeImpl clone() {
	DuctileDBEdgeImpl cloned = (DuctileDBEdgeImpl) super.clone();
	ElementUtils.setFinalField(cloned, DuctileDBEdgeImpl.class, "label", label);
	ElementUtils.setFinalField(cloned, DuctileDBEdgeImpl.class, "startVertexId", startVertexId);
	ElementUtils.setFinalField(cloned, DuctileDBEdgeImpl.class, "targetVertexId", targetVertexId);
	ElementUtils.setFinalField(cloned, DuctileDBEdgeImpl.class, "startVertex", null);
	ElementUtils.setFinalField(cloned, DuctileDBEdgeImpl.class, "targetVertex", null);
	return cloned;
    }
}

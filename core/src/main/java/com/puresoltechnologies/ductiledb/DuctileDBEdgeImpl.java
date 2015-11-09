package com.puresoltechnologies.ductiledb;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.tinkerpop.blueprints.Direction;

public class DuctileDBEdgeImpl implements DuctileDBEdge {

    private final DuctileDBGraphImpl graph;
    private final long id;
    private final String label;
    private final long startVertexId;
    private final long targetVertexId;
    private DuctileDBVertex startVertex = null;
    private DuctileDBVertex targetVertex = null;
    private final Map<String, Object> properties = new HashMap<>();

    public DuctileDBEdgeImpl(DuctileDBGraphImpl graph, long id, String label, long startVertexId, long targetVertexId,
	    Map<String, Object> properties) {
	super();
	this.graph = graph;
	this.id = id;
	this.label = label;
	this.startVertexId = startVertexId;
	this.targetVertexId = targetVertexId;
	this.properties.putAll(properties);
    }

    public DuctileDBEdgeImpl(DuctileDBGraphImpl graph, long id, String label, DuctileDBVertex startVertex,
	    long targetVertexId, Map<String, Object> properties) {
	super();
	this.graph = graph;
	this.id = id;
	this.label = label;
	this.startVertex = startVertex;
	this.startVertexId = startVertex.getId();
	this.targetVertexId = targetVertexId;
	this.properties.putAll(properties);
    }

    public DuctileDBEdgeImpl(DuctileDBGraphImpl graph, long id, String label, long startVertexId,
	    DuctileDBVertex targetVertex, Map<String, Object> properties) {
	super();
	this.graph = graph;
	this.id = id;
	this.label = label;
	this.startVertexId = startVertexId;
	this.targetVertex = targetVertex;
	this.targetVertexId = targetVertex.getId();
	this.properties.putAll(properties);
    }

    public DuctileDBEdgeImpl(DuctileDBGraphImpl graph, long id, String label, DuctileDBVertex startVertex,
	    DuctileDBVertex targetVertex, Map<String, Object> properties) {
	super();
	this.graph = graph;
	this.id = id;
	this.label = label;
	this.startVertexId = startVertex.getId();
	this.targetVertexId = targetVertex.getId();
	this.startVertex = startVertex;
	this.targetVertex = targetVertex;
	this.properties.putAll(properties);
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + (int) (id ^ (id >>> 32));
	result = prime * result + ((label == null) ? 0 : label.hashCode());
	result = prime * result + ((properties == null) ? 0 : properties.hashCode());
	result = prime * result + (int) (startVertexId ^ (startVertexId >>> 32));
	result = prime * result + (int) (targetVertexId ^ (targetVertexId >>> 32));
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
	DuctileDBEdgeImpl other = (DuctileDBEdgeImpl) obj;
	if (id != other.id)
	    return false;
	if (label == null) {
	    if (other.label != null)
		return false;
	} else if (!label.equals(other.label))
	    return false;
	if (properties == null) {
	    if (other.properties != null)
		return false;
	} else if (!properties.equals(other.properties))
	    return false;
	if (startVertexId != other.startVertexId)
	    return false;
	if (targetVertexId != other.targetVertexId)
	    return false;
	return true;
    }

    @Override
    public DuctileDBVertex getStartVertex() {
	return startVertex;
    }

    @Override
    public DuctileDBVertex getTargetVertex() {
	return targetVertex;
    }

    @Override
    public DuctileDBVertex getVertex(Direction direction) throws IllegalArgumentException {
	switch (direction) {
	case OUT:
	    if (startVertex == null) {
		startVertex = graph.getVertex(startVertexId);
	    }
	    return startVertex;
	case IN:
	    if (targetVertex == null) {
		targetVertex = graph.getVertex(targetVertexId);
	    }
	    return targetVertex;
	default:
	    throw new IllegalArgumentException("Direction '" + direction + "' is not supported.");
	}
    }

    @Override
    public String getLabel() {
	return label;
    }

    @Override
    public <T> T getProperty(String key) {
	@SuppressWarnings("unchecked")
	T t = (T) properties.get(key);
	return t;
    }

    @Override
    public Set<String> getPropertyKeys() {
	return properties.keySet();
    }

    @Override
    public void setProperty(String key, Object value) {
	graph.setEdgeProperty(this, key, value);
	properties.put(key, value);
    }

    @Override
    public <T> T removeProperty(String key) {
	@SuppressWarnings("unchecked")
	T value = (T) properties.get(key);
	graph.removeEdgeProperty(this, key);
	return value;
    }

    @Override
    public void remove() {
	graph.removeEdge(this);
    }

    @Override
    public Long getId() {
	return id;
    }

    @Override
    public String toString() {
	return "edge " + id + " (" + startVertexId + "->" + targetVertexId + "): label=" + label + "; properties="
		+ properties;
    }
}

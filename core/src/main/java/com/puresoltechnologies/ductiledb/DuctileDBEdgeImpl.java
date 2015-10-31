package com.puresoltechnologies.ductiledb;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

public class DuctileDBEdgeImpl implements DuctileDBEdge {

    private final DuctileDBGraph graph;
    private final long id;
    private final String label;
    private final long startVertexId;
    private final long targetVertexId;
    private DuctileDBVertex startVertex = null;
    private DuctileDBVertex targetVertex = null;
    private final Map<String, Object> properties = new HashMap<>();

    public DuctileDBEdgeImpl(DuctileDBGraph graph, long id, String label, long startVertexId, long targetVertexId,
	    Map<String, Object> properties) {
	super();
	this.graph = graph;
	this.id = id;
	this.label = label;
	this.startVertexId = startVertexId;
	this.targetVertexId = targetVertexId;
	this.properties.putAll(properties);
    }

    public DuctileDBEdgeImpl(DuctileDBGraph graph, long id, String label, DuctileDBVertex startVertex,
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
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
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
	// TODO
    }

    @Override
    public <T> T removeProperty(String key) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public void remove() {
	// TODO Auto-generated method stub

    }

    @Override
    public Long getId() {
	return id;
    }

}

package com.puresoltechnologies.hgraph;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

public class HGraphEdgeImpl implements HGraphEdge {

    private final String label;
    private final HGraphVertex startVertex;
    private final HGraphVertex targetVertex;
    private final Map<String, Object> properties = new HashMap<>();

    public HGraphEdgeImpl(String label, HGraphVertex startVertex, HGraphVertex targetVertex) {
	super();
	this.label = label;
	this.startVertex = startVertex;
	this.targetVertex = targetVertex;
	this.properties.putAll(properties);
    }

    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
	switch (direction) {
	case OUT:
	    return startVertex;
	case IN:
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
    public Object getId() {
	// TODO Auto-generated method stub
	return null;
    }

}

package com.puresoltechnologies.ductiledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

public class DuctileDBVertexImpl implements DuctileDBVertex {

    private final DuctileDBGraphImpl graph;
    private final long id;
    private final Set<String> labels = new HashSet<>();
    private final Map<String, Object> properties = new HashMap<>();

    public DuctileDBVertexImpl(DuctileDBGraphImpl hgraph, long id, Set<String> labels, Map<String, Object> properties) {
	this.graph = hgraph;
	this.id = id;
	this.labels.addAll(labels);
	this.properties.putAll(properties);
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((graph == null) ? 0 : graph.hashCode());
	result = prime * result + (int) (id ^ (id >>> 32));
	result = prime * result + ((labels == null) ? 0 : labels.hashCode());
	result = prime * result + ((properties == null) ? 0 : properties.hashCode());
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
	DuctileDBVertexImpl other = (DuctileDBVertexImpl) obj;
	if (graph == null) {
	    if (other.graph != null)
		return false;
	} else if (!graph.equals(other.graph))
	    return false;
	if (id != other.id)
	    return false;
	if (labels == null) {
	    if (other.labels != null)
		return false;
	} else if (!labels.equals(other.labels))
	    return false;
	if (properties == null) {
	    if (other.properties != null)
		return false;
	} else if (!properties.equals(other.properties))
	    return false;
	return true;
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... edgeLabels) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public VertexQuery query() {
	throw new UnsupportedOperationException("Querying of HGraph is not supported, yet.");
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex) {
	// TODO Auto-generated method stub
	return null;
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
	graph.setVertexProperty(id, key, value);
	properties.put(key, value);
    }

    @Override
    public <T> T removeProperty(String key) {
	@SuppressWarnings("unchecked")
	T value = (T) getProperty(key);
	graph.removeVertexProperty(id, key);
	return value;
    }

    @Override
    public void remove() {
	graph.removeVertex(this);
    }

    @Override
    public Long getId() {
	return id;
    }

    @Override
    public Iterable<String> getLabels() {
	return new Iterable<String>() {
	    @Override
	    public Iterator<String> iterator() {
		return labels.iterator();
	    }
	};
    }

    @Override
    public void addLabel(String label) {
	graph.addLabel(id, label);
	labels.add(label);
    }

    @Override
    public void removeLabel(String label) {
	graph.removeLabel(id, label);
	labels.remove(label);
    }

    @Override
    public boolean hasLabel(String label) {
	return labels.contains(label);
    }

}

package com.puresoltechnologies.ductiledb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

public class HGraphVertexImpl implements HGraphVertex {

    private final HGraphImpl hgraph;
    private final byte[] id;
    private final Set<String> labels = new HashSet<>();
    private final Map<String, Object> properties = new HashMap<>();

    public HGraphVertexImpl(HGraphImpl hgraph, byte[] id, Set<String> labels, Map<String, Object> properties) {
	this.hgraph = hgraph;
	this.id = id;
	this.labels.addAll(labels);
	this.properties.putAll(properties);
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + Arrays.hashCode(id);
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
	HGraphVertexImpl other = (HGraphVertexImpl) obj;
	if (!Arrays.equals(id, other.id))
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
	hgraph.setVertexProperty(id, key, value);
	properties.put(key, value);
    }

    @Override
    public <T> T removeProperty(String key) {
	@SuppressWarnings("unchecked")
	T value = (T) getProperty(key);
	hgraph.removeVertexProperty(id, key);
	return value;
    }

    @Override
    public void remove() {
	hgraph.removeVertex(this);
    }

    @Override
    public Object getId() {
	return HGraphImpl.decodeRowKey(id);
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
	hgraph.addLabel(id, label);
	labels.add(label);
    }

    @Override
    public void removeLabel(String label) {
	hgraph.removeLabel(id, label);
	labels.remove(label);
    }

    @Override
    public boolean hasLabel(String label) {
	return labels.contains(label);
    }

}

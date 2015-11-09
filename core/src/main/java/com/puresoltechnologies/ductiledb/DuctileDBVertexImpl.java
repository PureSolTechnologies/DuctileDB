package com.puresoltechnologies.ductiledb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
    private final List<DuctileDBEdge> edges = new ArrayList<>();

    public DuctileDBVertexImpl(DuctileDBGraphImpl hgraph, long id, Set<String> labels, Map<String, Object> properties,
	    List<DuctileDBEdge> edges) {
	this.graph = hgraph;
	this.id = id;
	this.labels.addAll(labels);
	this.properties.putAll(properties);
	this.edges.addAll(edges);
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((edges == null) ? 0 : edges.hashCode());
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
	if (edges == null) {
	    if (other.edges != null)
		return false;
	} else if (!edges.equals(other.edges))
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
    public Iterable<Edge> getEdges(Direction direction, String... edgeLabels) {
	List<Edge> edges = new ArrayList<>();
	List<String> labelList = Arrays.asList(edgeLabels);
	for (DuctileDBEdge edge : this.edges) {
	    if (labelList.contains(edge.getLabel())) {
		switch (direction) {
		case IN:
		    if (edge.getVertex(Direction.IN).getId().equals(getId())) {
			edges.add(edge);
		    }
		    break;
		case OUT:
		    if (edge.getVertex(Direction.OUT).getId().equals(getId())) {
			edges.add(edge);
		    }
		    break;
		case BOTH:
		    edges.add(edge);
		    break;
		default:
		    throw new IllegalArgumentException("Direction '" + direction + "' is not supported.");
		}
	    }
	}
	return edges;
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... edgeLabels) {
	List<Vertex> vertices = new ArrayList<>();
	List<String> labelList = Arrays.asList(edgeLabels);
	for (DuctileDBEdge edge : this.edges) {
	    if (labelList.contains(edge.getLabel())) {
		switch (direction) {
		case IN:
		    if (edge.getVertex(Direction.IN).getId().equals(getId())) {
			vertices.add(edge.getVertex(Direction.OUT));
		    }
		    break;
		case OUT:
		    if (edge.getVertex(Direction.OUT).getId().equals(getId())) {
			vertices.add(edge.getVertex(Direction.OUT));
		    }
		    break;
		case BOTH:
		    Vertex vertex = edge.getVertex(Direction.IN);
		    if (vertex.getId().equals(getId())) {
			vertices.add(edge.getVertex(Direction.OUT));
		    } else {
			vertices.add(vertex);
		    }
		    break;
		default:
		    throw new IllegalArgumentException("Direction '" + direction + "' is not supported.");
		}
	    }
	}
	return vertices;
    }

    @Override
    public VertexQuery query() {
	throw new UnsupportedOperationException("Querying of HGraph is not supported, yet.");
    }

    void addEdge(DuctileDBEdge edge) {
	edges.add(edge);
    }

    @Override
    public DuctileDBEdge addEdge(String label, Vertex inVertex) {
	DuctileDBEdge edge = graph.addEdge(this, inVertex, label);
	edges.add(edge);
	return edge;
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
	graph.setVertexProperty(this, key, value);
	properties.put(key, value);
    }

    @Override
    public <T> T removeProperty(String key) {
	@SuppressWarnings("unchecked")
	T value = (T) getProperty(key);
	graph.removeVertexProperty(this, key);
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
	graph.addLabel(this, label);
	labels.add(label);
    }

    @Override
    public void removeLabel(String label) {
	graph.removeLabel(this, label);
	labels.remove(label);
    }

    @Override
    public boolean hasLabel(String label) {
	return labels.contains(label);
    }

    @Override
    public String toString() {
	return "vertex " + id + ": labels=" + labels + "; properties=" + properties + "; edges=" + edges;
    }
}

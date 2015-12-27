package com.puresoltechnologies.ductiledb.core.tx;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.EdgeDirection;
import com.puresoltechnologies.ductiledb.api.exceptions.DuctileDBException;
import com.puresoltechnologies.ductiledb.core.DuctileDBAttachedEdge;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;

class DuctileDBCacheVertex extends DuctileDBCacheElement implements DuctileDBVertex {

    private final Set<String> labels = new HashSet<>();
    private final List<DuctileDBCacheEdge> edges = new ArrayList<>();

    public DuctileDBCacheVertex(DuctileDBGraphImpl graph, long id, Set<String> labels, Map<String, Object> properties,
	    List<DuctileDBCacheEdge> edges) {
	super(graph, id, properties);
	this.labels.addAll(labels);
	this.edges.addAll(edges);
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = super.hashCode();
	result = prime * result + ((edges == null) ? 0 : edges.hashCode());
	result = prime * result + ((labels == null) ? 0 : labels.hashCode());
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
	DuctileDBCacheVertex other = (DuctileDBCacheVertex) obj;
	if (edges == null) {
	    if (other.edges != null)
		return false;
	} else if (!edges.equals(other.edges))
	    return false;
	if (labels == null) {
	    if (other.labels != null)
		return false;
	} else if (!labels.equals(other.labels))
	    return false;
	return true;
    }

    @Override
    public Iterable<DuctileDBEdge> getEdges(EdgeDirection direction, String... edgeLabels) {
	List<DuctileDBEdge> edges = new ArrayList<>();
	List<String> labelList = Arrays.asList(edgeLabels);
	for (DuctileDBEdge cachedEdge : this.edges) {
	    if ((edgeLabels.length == 0) || (labelList.contains(cachedEdge.getLabel()))) {
		DuctileDBAttachedEdge edge = ElementUtils.toAttached(cachedEdge);
		switch (direction) {
		case IN:
		    if (edge.getVertex(EdgeDirection.IN).getId() == getId()) {
			edges.add(edge);
		    }
		    break;
		case OUT:
		    if (edge.getVertex(EdgeDirection.OUT).getId() == getId()) {
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
    public Iterable<DuctileDBVertex> getVertices(EdgeDirection direction, String... edgeLabels) {
	List<DuctileDBVertex> vertices = new ArrayList<>();
	List<String> labelList = Arrays.asList(edgeLabels);
	for (DuctileDBEdge edge : this.edges) {
	    if (labelList.contains(edge.getLabel())) {
		switch (direction) {
		case IN:
		    if (edge.getVertex(EdgeDirection.IN).getId() == getId()) {
			vertices.add(edge.getVertex(EdgeDirection.OUT));
		    }
		    break;
		case OUT:
		    if (edge.getVertex(EdgeDirection.OUT).getId() == getId()) {
			vertices.add(edge.getVertex(EdgeDirection.OUT));
		    }
		    break;
		case BOTH:
		    DuctileDBVertex vertex = edge.getVertex(EdgeDirection.IN);
		    if (vertex.getId() == getId()) {
			vertices.add(edge.getVertex(EdgeDirection.OUT));
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

    public void removeEdge(DuctileDBEdge edge) {
	edges.remove(edge);
    }

    @Override
    public DuctileDBCacheEdge addEdge(String label, DuctileDBVertex inVertex) {
	throw new DuctileDBException("This method cannot be used for a cached vertex.");
    }

    @Override
    public DuctileDBCacheEdge addEdge(String label, DuctileDBVertex inVertex, Map<String, Object> properties) {
	throw new DuctileDBException("This method cannot be used for a cached vertex.");
    }

    public void addEdge(DuctileDBCacheEdge edge) {
	edges.add(edge);
    }

    @Override
    public void remove() {
	getGraph().removeVertex(this);
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
	labels.add(label);
    }

    @Override
    public void removeLabel(String label) {
	labels.remove(label);
    }

    @Override
    public boolean hasLabel(String label) {
	return labels.contains(label);
    }

    @Override
    public String toString() {
	return getClass().getSimpleName() + " " + getId() + ": labels=" + labels + "; properties="
		+ getPropertiesString() + "; edges=" + edges;
    }

    @Override
    public DuctileDBCacheVertex clone() {
	DuctileDBCacheVertex cloned = (DuctileDBCacheVertex) super.clone();
	ElementUtils.setFinalField(cloned, DuctileDBCacheVertex.class, "labels", new HashSet<>(labels));
	List<DuctileDBEdge> clonedEdges = new ArrayList<>();
	for (DuctileDBEdge edge : edges) {
	    clonedEdges.add(edge.clone());
	}
	ElementUtils.setFinalField(cloned, DuctileDBCacheVertex.class, "edges", clonedEdges);
	return cloned;
    }
}

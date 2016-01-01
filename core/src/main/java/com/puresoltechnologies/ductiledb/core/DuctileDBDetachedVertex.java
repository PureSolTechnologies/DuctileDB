package com.puresoltechnologies.ductiledb.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.EdgeDirection;
import com.puresoltechnologies.ductiledb.api.exceptions.DuctileDBException;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;

public class DuctileDBDetachedVertex extends DuctileDBDetachedElement implements DuctileDBVertex {

    private final Set<String> types;
    private final List<DuctileDBEdge> edges;

    public DuctileDBDetachedVertex(DuctileDBGraphImpl graph, long id, Set<String> types, Map<String, Object> properties,
	    List<DuctileDBEdge> edges) {
	super(graph, id, properties);
	this.types = Collections.unmodifiableSet(types);
	this.edges = Collections.unmodifiableList(edges);
    }

    private void throwDetachedException() {
	throw new DuctileDBException("This vertex is detached from graph and cannot be altered.");
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = super.hashCode();
	result = prime * result + ((edges == null) ? 0 : edges.hashCode());
	result = prime * result + ((types == null) ? 0 : types.hashCode());
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
	DuctileDBDetachedVertex other = (DuctileDBDetachedVertex) obj;
	if (edges == null) {
	    if (other.edges != null)
		return false;
	} else if (!edges.equals(other.edges))
	    return false;
	if (types == null) {
	    if (other.types != null)
		return false;
	} else if (!types.equals(other.types))
	    return false;
	return true;
    }

    @Override
    public Iterable<DuctileDBEdge> getEdges(EdgeDirection direction, String... edgeTypes) {
	List<DuctileDBEdge> edges = new ArrayList<>();
	List<String> typeList = Arrays.asList(edgeTypes);
	for (DuctileDBEdge edge : this.edges) {
	    if ((edgeTypes.length == 0) || (typeList.contains(edge.getType()))) {
		switch (direction) {
		case IN:
		    if (edge.getTargetVertex().getId() == getId()) {
			edges.add(edge);
		    }
		    break;
		case OUT:
		    if (edge.getStartVertex().getId() == getId()) {
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
    public Iterable<DuctileDBVertex> getVertices(EdgeDirection direction, String... edgeTypes) {
	List<DuctileDBVertex> vertices = new ArrayList<>();
	List<String> typeList = Arrays.asList(edgeTypes);
	for (DuctileDBEdge edge : this.edges) {
	    if (typeList.contains(edge.getType())) {
		switch (direction) {
		case IN:
		    if (edge.getTargetVertex().getId() == getId()) {
			vertices.add(edge.getStartVertex());
		    }
		    break;
		case OUT:
		    if (edge.getStartVertex().getId() == getId()) {
			vertices.add(edge.getTargetVertex());
		    }
		    break;
		case BOTH:
		    DuctileDBVertex vertex = edge.getTargetVertex();
		    if (vertex.getId() == getId()) {
			vertices.add(edge.getStartVertex());
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
	throwDetachedException();
    }

    @Override
    public DuctileDBEdge addEdge(String type, DuctileDBVertex targetVertex, Map<String, Object> properties) {
	throwDetachedException();
	return null;
    }

    @Override
    protected <T> void setProperty(DuctileDBGraph graph, String key, T value) {
	throwDetachedException();
    }

    @Override
    public void removeProperty(DuctileDBGraph graph, String key) {
	throwDetachedException();
    }

    @Override
    public void remove() {
	throwDetachedException();
    }

    @Override
    public Iterable<String> getTypes() {
	return new Iterable<String>() {
	    @Override
	    public Iterator<String> iterator() {
		return types.iterator();
	    }
	};
    }

    @Override
    public void addType(String type) {
	throwDetachedException();
    }

    @Override
    public void removeType(String type) {
	throwDetachedException();
    }

    @Override
    public boolean hasType(String type) {
	return types.contains(type);
    }

    @Override
    public String toString() {
	return getClass().getSimpleName() + " " + getId() + ": types=" + types + "; properties=" + getPropertiesString()
		+ "; edges=" + edges;
    }

    @Override
    public DuctileDBDetachedVertex clone() {
	DuctileDBDetachedVertex cloned = (DuctileDBDetachedVertex) super.clone();
	ElementUtils.setFinalField(cloned, DuctileDBDetachedVertex.class, "types", new HashSet<>(types));
	List<DuctileDBEdge> clonedEdges = new ArrayList<>();
	for (DuctileDBEdge edge : edges) {
	    clonedEdges.add(edge.clone());
	}
	ElementUtils.setFinalField(cloned, DuctileDBDetachedVertex.class, "edges", clonedEdges);
	return cloned;
    }
}

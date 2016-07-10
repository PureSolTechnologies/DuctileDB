package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.puresoltechnologies.ductiledb.api.DuctileDBException;
import com.puresoltechnologies.ductiledb.api.graph.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.graph.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.graph.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBAttachedEdge;
import com.puresoltechnologies.ductiledb.core.graph.utils.ElementUtils;

class DuctileDBCacheVertex extends DuctileDBCacheElement implements DuctileDBVertex {

    private final Set<String> types = new HashSet<>();
    private final List<Long> edges = new ArrayList<>();

    public DuctileDBCacheVertex(DuctileDBTransactionImpl transaction, long id, Set<String> types,
	    Map<String, Object> properties, List<DuctileDBCacheEdge> edges) {
	super(transaction, id, properties);
	if (types == null) {
	    throw new IllegalArgumentException("Types must not be null.");
	}
	if (edges == null) {
	    throw new IllegalArgumentException("Edges must not be null.");
	}
	this.types.addAll(types);
	edges.forEach(edge -> this.edges.add(edge.getId()));
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
	DuctileDBCacheVertex other = (DuctileDBCacheVertex) obj;
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
	for (long edgeId : this.edges) {
	    if (getTransaction().wasEdgeRemoved(edgeId)) {
		continue;
	    }
	    DuctileDBEdge cachedEdge = getTransaction().getEdge(edgeId);
	    if ((edgeTypes.length == 0) || (typeList.contains(cachedEdge.getType()))) {
		DuctileDBAttachedEdge edge = ElementUtils.toAttached(cachedEdge);
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
	for (long edgeId : this.edges) {
	    DuctileDBEdge edge = getTransaction().getEdge(edgeId);
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
	edges.remove(edge.getId());
    }

    @Override
    public DuctileDBCacheEdge addEdge(String type, DuctileDBVertex targetVertex, Map<String, Object> properties) {
	throw new DuctileDBException("This method cannot be used for a cached vertex.");
    }

    public void addEdge(DuctileDBCacheEdge edge) {
	edges.add(edge.getId());
    }

    @Override
    public void remove() {
	getTransaction().removeVertex(this);
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
	types.add(type);
    }

    @Override
    public void removeType(String type) {
	types.remove(type);
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
    public DuctileDBCacheVertex clone() {
	DuctileDBCacheVertex cloned = (DuctileDBCacheVertex) super.clone();
	ElementUtils.setFinalField(cloned, DuctileDBCacheVertex.class, "types", new HashSet<>(types));
	List<Long> clonedEdges = new ArrayList<>();
	for (long edgeId : edges) {
	    clonedEdges.add(edgeId);
	}
	ElementUtils.setFinalField(cloned, DuctileDBCacheVertex.class, "edges", clonedEdges);
	return cloned;
    }
}

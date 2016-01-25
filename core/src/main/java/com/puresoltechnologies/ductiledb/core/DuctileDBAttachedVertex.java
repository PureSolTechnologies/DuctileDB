package com.puresoltechnologies.ductiledb.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.tx.DuctileDBTransactionImpl;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;

public class DuctileDBAttachedVertex extends DuctileDBAttachedElement implements DuctileDBVertex {

    public DuctileDBAttachedVertex(DuctileDBGraphImpl graph, DuctileDBTransactionImpl transaction, long id) {
	super(graph, transaction, id);
    }

    @Override
    public Iterable<DuctileDBEdge> getEdges(EdgeDirection direction, String... edgeTypes) {
	return getTransaction().getEdges(getId(), direction, edgeTypes);
    }

    @Override
    public Iterable<DuctileDBVertex> getVertices(EdgeDirection direction, String... edgeTypes) {
	List<DuctileDBVertex> vertices = new ArrayList<>();
	List<String> typeList = Arrays.asList(edgeTypes);
	for (DuctileDBEdge edge : getTransaction().getVertexEdges(getId())) {
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
	getTransaction().removeEdge(edge);
    }

    @Override
    public DuctileDBEdge addEdge(String type, DuctileDBVertex targetVertex, Map<String, Object> properties) {
	return getTransaction().addEdge(this, targetVertex, type, properties);
    }

    @Override
    public void remove() {
	getTransaction().removeVertex(this);
    }

    @Override
    public Iterable<String> getTypes() {
	return getTransaction().getVertexTypes(getId());
    }

    @Override
    public void addType(String type) {
	getTransaction().addType(this, type);
    }

    @Override
    public void removeType(String type) {
	getTransaction().removeType(this, type);
    }

    @Override
    public boolean hasType(String type) {
	return getTransaction().hasType(getId(), type);
    }

    @Override
    public Set<String> getPropertyKeys() {
	return getTransaction().getVertexPropertyKeys(getId());
    }

    @Override
    public <T> void setProperty(String key, T value) {
	getTransaction().setProperty(this, key, value);
    }

    @Override
    public <T> T getProperty(String key) {
	return getTransaction().getVertexProperty(getId(), key);
    }

    @Override
    public void removeProperty(String key) {
	getTransaction().removeProperty(this, key);
    }

    @Override
    public String toString() {
	return getClass().getSimpleName() + " " + getId() + ": types=" + ElementUtils.getTypes(this) + "; properties="
		+ getPropertiesString() + "; edges=" + getEdges(EdgeDirection.BOTH);
    }

    @Override
    public DuctileDBAttachedVertex clone() {
	DuctileDBAttachedVertex cloned = (DuctileDBAttachedVertex) super.clone();
	ElementUtils.setFinalField(cloned, DuctileDBAttachedVertex.class, "types", ElementUtils.getTypes(this));
	ElementUtils.setFinalField(cloned, DuctileDBAttachedVertex.class, "edges", ElementUtils.getEdges(this));
	return cloned;
    }
}

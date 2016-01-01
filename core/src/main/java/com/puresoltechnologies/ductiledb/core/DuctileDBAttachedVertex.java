package com.puresoltechnologies.ductiledb.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;

public class DuctileDBAttachedVertex extends DuctileDBAttachedElement implements DuctileDBVertex {

    public DuctileDBAttachedVertex(DuctileDBGraphImpl graph, long id) {
	super(graph, id);
    }

    @Override
    public Iterable<DuctileDBEdge> getEdges(EdgeDirection direction, String... edgeTypes) {
	return getCurrentTransaction().getEdges(getId(), direction, edgeTypes);
    }

    @Override
    public Iterable<DuctileDBVertex> getVertices(EdgeDirection direction, String... edgeTypes) {
	List<DuctileDBVertex> vertices = new ArrayList<>();
	List<String> typeList = Arrays.asList(edgeTypes);
	for (DuctileDBEdge edge : getCurrentTransaction().getVertexEdges(getId())) {
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
	getCurrentTransaction().removeEdge(edge);
    }

    @Override
    public DuctileDBEdge addEdge(String type, DuctileDBVertex targetVertex, Map<String, Object> properties) {
	return getCurrentTransaction().addEdge(this, targetVertex, type, properties);
    }

    @Override
    public void remove() {
	getCurrentTransaction().removeVertex(this);
    }

    @Override
    public Iterable<String> getTypes() {
	return getCurrentTransaction().getVertexTypes(getId());
    }

    @Override
    public void addType(String type) {
	getCurrentTransaction().addType(this, type);
    }

    @Override
    public void removeType(String type) {
	getCurrentTransaction().removeType(this, type);
    }

    @Override
    public boolean hasType(String type) {
	return getCurrentTransaction().hasType(getId(), type);
    }

    @Override
    public Set<String> getPropertyKeys() {
	return getCurrentTransaction().getVertexPropertyKeys(getId());
    }

    @Override
    public <T> void setProperty(String key, T value) {
	getCurrentTransaction().setProperty(this, key, value);
    }

    @Override
    public <T> T getProperty(String key) {
	return getCurrentTransaction().getVertexProperty(getId(), key);
    }

    @Override
    public void removeProperty(String key) {
	getCurrentTransaction().removeProperty(this, key);
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

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
    public Iterable<DuctileDBEdge> getEdges(EdgeDirection direction, String... edgeLabels) {
	return getCurrentTransaction().getEdges(getId(), direction, edgeLabels);
    }

    @Override
    public Iterable<DuctileDBVertex> getVertices(EdgeDirection direction, String... edgeLabels) {
	List<DuctileDBVertex> vertices = new ArrayList<>();
	List<String> labelList = Arrays.asList(edgeLabels);
	for (DuctileDBEdge edge : getCurrentTransaction().getVertexEdges(getId())) {
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
	getCurrentTransaction().removeEdge(edge);
    }

    @Override
    public DuctileDBEdge addEdge(String label, DuctileDBVertex inVertex) {
	return getCurrentTransaction().addEdge(this, inVertex, label);
    }

    @Override
    public DuctileDBEdge addEdge(String label, DuctileDBVertex inVertex, Map<String, Object> properties) {
	return getCurrentTransaction().addEdge(this, inVertex, label, properties);
    }

    @Override
    public void remove() {
	getCurrentTransaction().removeVertex(this);
    }

    @Override
    public Iterable<String> getLabels() {
	return getCurrentTransaction().getVertexLabels(getId());
    }

    @Override
    public void addLabel(String label) {
	getCurrentTransaction().addLabel(this, label);
    }

    @Override
    public void removeLabel(String label) {
	getCurrentTransaction().removeLabel(this, label);
    }

    @Override
    public boolean hasLabel(String label) {
	return getCurrentTransaction().hasLabel(getId(), label);
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
	return getClass().getSimpleName() + " " + getId() + ": labels=" + ElementUtils.getLabels(this) + "; properties="
		+ getPropertiesString() + "; edges=" + getEdges(EdgeDirection.BOTH);
    }

    @Override
    public DuctileDBAttachedVertex clone() {
	DuctileDBAttachedVertex cloned = (DuctileDBAttachedVertex) super.clone();
	ElementUtils.setFinalField(cloned, DuctileDBAttachedVertex.class, "labels", ElementUtils.getLabels(this));
	ElementUtils.setFinalField(cloned, DuctileDBAttachedVertex.class, "edges", ElementUtils.getEdges(this));
	return cloned;
    }
}

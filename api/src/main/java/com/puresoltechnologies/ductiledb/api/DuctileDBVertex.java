package com.puresoltechnologies.ductiledb.api;

import java.util.Map;

public interface DuctileDBVertex extends DuctileDBElement, Cloneable {

    public Iterable<String> getLabels();

    public void addLabel(String label);

    public void removeLabel(String label);

    public boolean hasLabel(String label);

    public DuctileDBEdge addEdge(String label, DuctileDBVertex inVertex);

    public DuctileDBEdge addEdge(String label, DuctileDBVertex inVertex, Map<String, Object> properties);

    public Iterable<DuctileDBVertex> getVertices(EdgeDirection direction, String... edgeLabels);

    public Iterable<DuctileDBEdge> getEdges(EdgeDirection direction, String... edgeLabels);

    public DuctileDBVertex clone();
}

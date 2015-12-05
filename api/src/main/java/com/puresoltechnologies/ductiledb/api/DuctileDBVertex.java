package com.puresoltechnologies.ductiledb.api;

public interface DuctileDBVertex extends DuctileDBElement {

    public Iterable<String> getLabels();

    public void addLabel(String label);

    public void removeLabel(String label);

    public boolean hasLabel(String label);

    public DuctileDBEdge addEdge(String label, DuctileDBVertex inVertex);

    public Iterable<DuctileDBVertex> getVertices(EdgeDirection direction, String... edgeLabels);

    public Iterable<DuctileDBEdge> getEdges(EdgeDirection direction, String... edgeLabels);

}

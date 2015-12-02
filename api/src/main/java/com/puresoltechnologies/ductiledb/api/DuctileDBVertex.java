package com.puresoltechnologies.ductiledb.api;

import java.util.Set;

public interface DuctileDBVertex {

    public Long getId();

    public Iterable<String> getLabels();

    public void addLabel(String label);

    public void removeLabel(String label);

    public boolean hasLabel(String label);

    public DuctileDBEdge addEdge(String label, DuctileDBVertex inVertex);

    public Set<String> getPropertyKeys();

    public void setProperty(String key, Object value);

    public <T> T removeProperty(String key);

    public void remove();

    public <T> T getProperty(String key);

    public Iterable<DuctileDBVertex> getVertices(EdgeDirection direction, String... edgeLabels);

    public Iterable<DuctileDBEdge> getEdges(EdgeDirection direction, String... edgeLabels);

}

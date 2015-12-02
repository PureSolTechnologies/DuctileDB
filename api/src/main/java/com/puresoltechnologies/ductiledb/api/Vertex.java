package com.puresoltechnologies.ductiledb.api;

import java.util.Set;

public interface Vertex {

    public Long getId();

    public Iterable<String> getLabels();

    public void addLabel(String label);

    public void removeLabel(String label);

    public boolean hasLabel(String label);

    public Edge addEdge(String label, Vertex inVertex);

    public Set<String> getPropertyKeys();

    public void setProperty(String key, Object value);

    public <T> T removeProperty(String key);

    public void remove();

    public <T> T getProperty(String key);

    public Iterable<Vertex> getVertices(Direction direction, String... edgeLabels);

    public Iterable<Edge> getEdges(Direction direction, String... edgeLabels);

}

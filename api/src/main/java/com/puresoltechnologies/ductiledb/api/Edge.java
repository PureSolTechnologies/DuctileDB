package com.puresoltechnologies.ductiledb.api;

public interface Edge {

    public Long getId();

    public Vertex getStartVertex();

    public Vertex getTargetVertex();

    public Vertex getVertex(Direction direction) throws IllegalArgumentException;

    public String getLabel();

    public Iterable<String> getPropertyKeys();

    public <T> T getProperty(String key);

    public void setProperty(String key, Object value);

    public <T> T removeProperty(String key);

    public void remove();

}

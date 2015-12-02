package com.puresoltechnologies.ductiledb.api;

public interface DuctileDBEdge {

    public Long getId();

    public DuctileDBVertex getStartVertex();

    public DuctileDBVertex getTargetVertex();

    public DuctileDBVertex getVertex(EdgeDirection direction) throws IllegalArgumentException;

    public String getLabel();

    public Iterable<String> getPropertyKeys();

    public <T> T getProperty(String key);

    public void setProperty(String key, Object value);

    public <T> T removeProperty(String key);

    public void remove();

}

package com.puresoltechnologies.ductiledb.api;

public interface DuctileDBEdge extends DuctileDBElement, Cloneable {

    public DuctileDBVertex getStartVertex();

    public DuctileDBVertex getTargetVertex();

    public DuctileDBVertex getVertex(EdgeDirection direction) throws IllegalArgumentException;

    public String getLabel();

    public DuctileDBEdge clone();
}

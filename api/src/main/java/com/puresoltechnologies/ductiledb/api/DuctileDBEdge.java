package com.puresoltechnologies.ductiledb.api;

public interface DuctileDBEdge extends DuctileDBElement {

    public DuctileDBVertex getStartVertex();

    public DuctileDBVertex getTargetVertex();

    public DuctileDBVertex getVertex(EdgeDirection direction) throws IllegalArgumentException;

    public String getType();

}

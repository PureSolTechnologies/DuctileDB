package com.puresoltechnologies.ductiledb.core.graph;

/**
 * This interface provides access to the standard functionality of an edge.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DuctileDBEdge extends DuctileDBElement, Cloneable {

    /**
     * This method returns the start vertex of the edge.
     * 
     * @return A {@link DuctileDBVertex} is returned.
     */
    public DuctileDBVertex getStartVertex();

    /**
     * This method returns the target vertex of the edge.
     * 
     * @return A {@link DuctileDBVertex} is returned.
     */
    public DuctileDBVertex getTargetVertex();

    /**
     * This method returns the type of the edge. The type is used to distinguish
     * the different edges from each other and also to bind schema information
     * and constraints to it.
     * 
     * @return A {@link String} with the name of type is returned.
     */
    public String getType();

    @Override
    public DuctileDBEdge clone();
}

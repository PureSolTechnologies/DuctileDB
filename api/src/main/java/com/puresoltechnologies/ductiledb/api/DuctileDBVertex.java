package com.puresoltechnologies.ductiledb.api;

import java.util.Map;

/**
 * This interface provides the functionality of a DuctileDB vertex.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DuctileDBVertex extends DuctileDBElement, Cloneable {

    /**
     * This method returns all types of the vertex. The type is used to
     * distinguish the different edges from each other and also to bind schema
     * information and constraints to it.
     * 
     * @return An {@link Iterable} of {@link String} is returned containing the
     *         type names.
     */
    public Iterable<String> getTypes();

    /**
     * This add a new type to the vertex.
     * 
     * @param type
     *            is a {@link String} containing the name of the type.
     */
    public void addType(String type);

    /**
     * This method removed a type from the vertex. The removal does not remove
     * the properties bound to the type, but the type name only.
     * 
     * @param type
     *            is a {@link String} containing the name of the type.
     */
    public void removeType(String type);

    /**
     * Checks whether this vertex has a certain type.
     * 
     * @param type
     *            is a {@link String} containing the name of the type.
     * @return <code>true</code> is returned in case the type is available.
     *         <code>false</code> is returned otherwise.
     */
    public boolean hasType(String type);

    /**
     * This method adds a new edge to the vertex.
     * 
     * @param type
     *            is the type name of the edge.
     * @param targetVertex
     *            is the target vertex of the edge.
     * @param properties
     *            is a {@link Map} of properties to be added to the edge.
     * @return A newly created {@link DuctileDBEdge} object is returned.
     */
    public DuctileDBEdge addEdge(String type, DuctileDBVertex targetVertex, Map<String, Object> properties);

    /**
     * This method returns all vertices which are of defined type and direction.
     * 
     * @param direction
     *            is the {@link EdgeDirection} to check for. Only vertices of
     *            edges are returned which are pointing to this direction.
     * @param edgeTypes
     *            is an array of {@link String} containing a list of edge types.
     *            Only vertices of edges are returned which have at least one of
     *            these types.
     * @return An {@link Iterable} of {@link DuctileDBVertex} is returned
     *         containing the vertices found.
     */
    public Iterable<DuctileDBVertex> getVertices(EdgeDirection direction, String... edgeTypes);

    /**
     * This method returns all edges which are of defined type and direction.
     * 
     * @param direction
     *            is the {@link EdgeDirection} to check for. Only edges are
     *            returned which are pointing to this direction.
     * @param edgeTypes
     *            is an array of {@link String} containing a list of edge types.
     *            Only edges which have at least one of these types are
     *            returned.
     * @return An {@link Iterable} of {@link DuctileDBVertex} is returned
     *         containing the vertices found.
     */
    public Iterable<DuctileDBEdge> getEdges(EdgeDirection direction, String... edgeTypes);

    @Override
    public DuctileDBVertex clone();
}

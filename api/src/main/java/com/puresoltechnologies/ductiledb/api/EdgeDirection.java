package com.puresoltechnologies.ductiledb.api;

/**
 * This enum is used to specify the direction of an edge on a vertex.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public enum EdgeDirection {

    /**
     * Specifies that an edge is incoming for the referencing vertex.
     */
    IN,
    /**
     * Specifies that an edge is outgoing for the referencing vertex.
     */
    OUT,
    /**
     * Specifies that an edge can be either incoming or outgoing for the
     * referencing vertex.
     */
    BOTH;

}

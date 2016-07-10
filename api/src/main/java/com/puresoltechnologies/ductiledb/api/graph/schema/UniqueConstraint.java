package com.puresoltechnologies.ductiledb.api.graph.schema;

/**
 * Defines on what level a uniqueness is required.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public enum UniqueConstraint {

    /**
     * No unique constraint at all.
     */
    NONE,
    /**
     * Uniqueness is to be assured globally.
     */
    GLOBAL,
    /**
     * Uniqueness is to be assured per type.
     */
    TYPE;

}

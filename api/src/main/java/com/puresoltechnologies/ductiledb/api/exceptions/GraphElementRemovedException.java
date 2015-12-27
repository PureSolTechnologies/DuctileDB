package com.puresoltechnologies.ductiledb.api.exceptions;

/**
 * This exception is thrown in cases where an element is tried to be
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class GraphElementRemovedException extends DuctileDBException {

    private static final long serialVersionUID = -7577785763105638622L;

    public GraphElementRemovedException(String message, Throwable cause) {
	super(message, cause);
    }

    public GraphElementRemovedException(String message) {
	super(message);
    }

}

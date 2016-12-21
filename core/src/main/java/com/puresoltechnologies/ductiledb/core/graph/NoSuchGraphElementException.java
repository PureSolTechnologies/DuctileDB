package com.puresoltechnologies.ductiledb.core.graph;

import com.puresoltechnologies.ductiledb.backend.DuctileDBException;

public class NoSuchGraphElementException extends DuctileDBException {

    private static final long serialVersionUID = -5978938436348831894L;

    public NoSuchGraphElementException(String message, Throwable cause) {
	super(message, cause);
    }

    public NoSuchGraphElementException(String message) {
	super(message);
    }

}

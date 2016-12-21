package com.puresoltechnologies.ductiledb.core.graph.manager;

import com.puresoltechnologies.ductiledb.backend.DuctileDBException;
import com.puresoltechnologies.ductiledb.core.graph.schema.DuctileDBSchemaManager;

/**
 * This exception is thrown by {@link DuctileDBSchemaManager} in cases of
 * unexpected issues.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class DuctileDBSchemaManagerException extends DuctileDBException {

    private static final long serialVersionUID = 1587855174775753518L;

    public DuctileDBSchemaManagerException(String message, Throwable cause) {
	super(message, cause);
    }

    public DuctileDBSchemaManagerException(String message) {
	super(message);
    }

}

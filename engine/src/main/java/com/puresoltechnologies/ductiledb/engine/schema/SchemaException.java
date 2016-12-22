package com.puresoltechnologies.ductiledb.engine.schema;

public class SchemaException extends Exception {

    private static final long serialVersionUID = 2897365490776991152L;

    public SchemaException(String message) {
	super(message);
    }

    public SchemaException(String message, Throwable cause) {
	super(message, cause);
    }

}

package com.puresoltechnologies.ductiledb.storage.engine.schema;

public class SchemaException extends Exception {

    private static final long serialVersionUID = 2897365490776991152L;

    public SchemaException(Throwable cause) {
	super(cause);
    }

    public SchemaException(String message, Throwable cause) {
	super(message, cause);
    }

}

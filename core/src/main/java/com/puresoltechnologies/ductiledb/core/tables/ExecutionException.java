package com.puresoltechnologies.ductiledb.core.tables;

public class ExecutionException extends Exception {

    private static final long serialVersionUID = -7473900948011730384L;

    public ExecutionException(String message, Throwable cause) {
	super(message, cause);
    }

    public ExecutionException(String message) {
	super(message);
    }

}

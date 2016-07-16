package com.puresoltechnologies.ductiledb.storage.api;

/**
 * This exception is thrown as parent exception in cases of storage issues.
 * 
 * @author Rick-Rainer Ludwig
 */
public class StorageException extends Exception {

    private static final long serialVersionUID = 1001603477214335077L;

    public StorageException(String message, Throwable cause) {
	super(message, cause);
    }

    public StorageException(String message) {
	super(message);
    }

}

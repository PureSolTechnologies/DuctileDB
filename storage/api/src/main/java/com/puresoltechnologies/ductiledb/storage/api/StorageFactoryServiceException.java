package com.puresoltechnologies.ductiledb.storage.api;

/**
 * This exception is thrown in cases where no storage factory service could be
 * found or other issues with the service are experienced.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class StorageFactoryServiceException extends StorageException {

    private static final long serialVersionUID = -850950400183507146L;

    public StorageFactoryServiceException(String message, Throwable cause) {
	super(message, cause);
    }

    public StorageFactoryServiceException(String message) {
	super(message);
    }

}

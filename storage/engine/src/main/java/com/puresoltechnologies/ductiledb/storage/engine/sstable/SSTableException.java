package com.puresoltechnologies.ductiledb.storage.engine.sstable;

public class SSTableException extends Exception {

    private static final long serialVersionUID = 2036199219396745989L;

    public SSTableException(String message, Throwable cause) {
	super(message, cause);
    }

    public SSTableException(String message) {
	super(message);
    }

}

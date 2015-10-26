package com.puresoltechnologies.hgraph;

/**
 * General purpose exception for all HGraph related issues.
 * 
 * @author Rick-Rainer Ludwig
 */
public class HGraphException extends RuntimeException {

    private static final long serialVersionUID = -2552842856696970076L;

    public HGraphException(String message, Throwable cause) {
	super(message, cause);
    }

    public HGraphException(String message) {
	super(message);
    }

}

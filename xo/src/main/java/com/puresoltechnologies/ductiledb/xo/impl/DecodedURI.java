package com.puresoltechnologies.ductiledb.xo.impl;

import java.io.File;
import java.net.URI;

import com.buschmais.xo.api.XOException;

/**
 * This class is used to take a URI and to check and decode it as DuctileDB URI.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DecodedURI {

    /**
     * This constant contains a protocol for DuctileDB store provider.
     */
    static final String DUCTILEDB_SCHEME = "ductiledb";

    private final URI uri;
    private File path;
    private String namespace;

    public DecodedURI(URI uri) {
	super();
	this.uri = uri;
	checkAndDecodeURI();
    }

    private void checkAndDecodeURI() {
	String scheme = uri.getScheme();
	if (!DUCTILEDB_SCHEME.equals(scheme)) {
	    throw new XOException("Scheme '" + scheme + "' does not match expected scheme 'ductiledb'.");
	}
	String host = uri.getHost();
	if (host != null) {
	    throw new XOException("A host name is not supported in DuctileDB URI.");
	}
	int port = uri.getPort();
	if (port != -1) {
	    throw new XOException("A port number is not supported in DuctileDB URI.");
	}
	path = new File(uri.getPath());
	String query = uri.getQuery();
	if (query != null) {
	    String[] properties = query.split(";");
	    for (String property : properties) {
		String[] parts = property.split("=");
		if (parts.length != 2) {
		    throw new XOException("Unsupported property format '" + property + "' found.");
		}
		String key = parts[0];
		String value = parts[1];
		switch (key) {
		case "namespace":
		    namespace = value;
		    break;
		default:
		    throw new XOException("Unsupported property key '" + key + "' found.");
		}
	    }
	} else {
	    namespace = DUCTILEDB_SCHEME;
	}
    }

    public URI getUri() {
	return uri;
    }

    public File getPath() {
	return path;
    }

    public String getNamespace() {
	return namespace;
    }

}

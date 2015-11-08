package com.puresoltechnologies.ductiledb.xo.api;

import java.net.URI;

import com.buschmais.xo.api.XOException;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.buschmais.xo.spi.bootstrap.XODatastoreProvider;
import com.buschmais.xo.spi.datastore.Datastore;
import com.puresoltechnologies.ductiledb.xo.impl.DuctileDBStore;
import com.puresoltechnologies.ductiledb.xo.impl.DuctileDBStoreSession;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileDBEdgeMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileDBVertexMetadata;

/**
 * This class implements the XO XODatastoreProvider for Titan database.
 * 
 * @author Rick-Rainer Ludwig
 */
public class TitanXOProvider
	implements
	XODatastoreProvider<DuctileDBVertexMetadata, String, DuctileDBEdgeMetadata, String> {

    /**
     * This constant contains {@value #TITAN_SCHEME_PREFIX} as prefix for all
     * URI protocols referencing a Titan store provider.
     */
    private static final String TITAN_SCHEME_PREFIX = "titan-";

    /**
     * This constant contains {@value #TITAN_CASSANDRA_SCHEME} as protocol a
     * Titan store provider on Cassandra.
     */
    private static final String TITAN_CASSANDRA_SCHEME = TITAN_SCHEME_PREFIX
	    + "cassandra";

    @Override
    public Datastore<DuctileDBStoreSession, DuctileDBVertexMetadata, String, DuctileDBEdgeMetadata, String> createDatastore(
	    XOUnit xoUnit) {
	if (xoUnit == null) {
	    throw new IllegalArgumentException("CdoUnit must not be null!");
	}
	URI uri = xoUnit.getUri();
	if (uri == null) {
	    throw new XOException("No URI is specified for the store.");
	}
	String scheme = uri.getScheme();
	if (!scheme.startsWith(TITAN_SCHEME_PREFIX)) {
	    throw new XOException("Only URIs starting with '"
		    + TITAN_SCHEME_PREFIX + "' are supported by this store.");
	}
	String host = uri.getHost();
	int port = uri.getPort();
	String keyspace = DuctileDBStore.retrieveKeyspaceFromURI(uri);
	switch (scheme) {
	case TITAN_CASSANDRA_SCHEME:
	    return new DuctileDBStore(host, port, keyspace);
	default:
	    throw new XOException("Scheme '" + scheme
		    + "' is not supported by this store.");
	}
    }
}

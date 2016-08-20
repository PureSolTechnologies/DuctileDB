package com.puresoltechnologies.ductiledb.xo.api;

import java.io.IOException;
import java.net.URI;

import com.buschmais.xo.api.XOException;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.buschmais.xo.spi.bootstrap.XODatastoreProvider;
import com.buschmais.xo.spi.datastore.Datastore;
import com.puresoltechnologies.ductiledb.xo.impl.DuctileStore;
import com.puresoltechnologies.ductiledb.xo.impl.DuctileStoreSession;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileEdgeMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileVertexMetadata;

/**
 * This class implements the XO XODatastoreProvider for DuctileDB database.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileXOProvider
	implements XODatastoreProvider<DuctileVertexMetadata, String, DuctileEdgeMetadata, String> {

    @Override
    public Datastore<DuctileStoreSession, DuctileVertexMetadata, String, DuctileEdgeMetadata, String> createDatastore(
	    XOUnit xoUnit) {
	if (xoUnit == null) {
	    throw new IllegalArgumentException("XOUnit must not be null!");
	}
	URI uri = xoUnit.getUri();
	if (uri == null) {
	    throw new XOException("No URI is specified for the store.");
	}
	try {
	    return new DuctileStore(uri.toURL());
	} catch (IOException e) {
	    throw new XOException("Could not initialize store.", e);
	}
    }
}

package com.puresoltechnologies.ductiledb.xo.api;

import java.net.URI;

import com.buschmais.xo.api.XOException;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.buschmais.xo.spi.bootstrap.XODatastoreProvider;
import com.buschmais.xo.spi.datastore.Datastore;
import com.puresoltechnologies.ductiledb.xo.impl.DecodedURI;
import com.puresoltechnologies.ductiledb.xo.impl.DuctileDBStore;
import com.puresoltechnologies.ductiledb.xo.impl.DuctileDBStoreSession;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileDBEdgeMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileDBVertexMetadata;

/**
 * This class implements the XO XODatastoreProvider for Titan database.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBXOProvider
	implements XODatastoreProvider<DuctileDBVertexMetadata, String, DuctileDBEdgeMetadata, String> {

    @Override
    public Datastore<DuctileDBStoreSession, DuctileDBVertexMetadata, String, DuctileDBEdgeMetadata, String> createDatastore(
	    XOUnit xoUnit) {
	if (xoUnit == null) {
	    throw new IllegalArgumentException("XOUnit must not be null!");
	}
	URI uri = xoUnit.getUri();
	if (uri == null) {
	    throw new XOException("No URI is specified for the store.");
	}
	DecodedURI decodedURI = new DecodedURI(uri);
	return new DuctileDBStore(decodedURI.getPath(), decodedURI.getNamespace());
    }
}

package com.puresoltechnologies.ductiledb.xo.impl;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.buschmais.xo.api.XOException;
import com.buschmais.xo.spi.datastore.Datastore;
import com.buschmais.xo.spi.datastore.DatastoreMetadataFactory;
import com.buschmais.xo.spi.metadata.type.TypeMetadata;
import com.puresoltechnologies.ductiledb.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.GraphFactory;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileDBEdgeMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileDBVertexMetadata;

/**
 * <p>
 * This class implements an XO Datastore for Titan on Cassandra.
 * </p>
 * <p>
 * For details have a look to
 * <a href="https://github.com/thinkaurelius/titan/wiki/Using-Cassandra" >https:
 * //github.com/thinkaurelius/titan/wiki/Using-Cassandra</a>
 * </p>
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBStore
	implements Datastore<DuctileDBStoreSession, DuctileDBVertexMetadata, String, DuctileDBEdgeMetadata, String> {

    private static final Logger logger = LoggerFactory.getLogger(DuctileDBStore.class);

    /**
     * This constant contains the default name of the Titan keyspace which is
     * set to {@value #DEFAULT_TITAN_KEYSPACE}.
     */
    public static final String DEFAULT_TITAN_KEYSPACE = "titan";

    /**
     * This constant contains the default port
     * {@value #DEFAULT_CASSANDRA_THRIFT_PORT} for the Thrift interface of
     * Cassandra.
     */
    public static final int DEFAULT_CASSANDRA_THRIFT_PORT = 9160;

    /**
     * This constant contains the name of the index to be used for properties.
     */
    public static final String INDEX_NAME = "standard";

    /**
     * This is a helper method to retrieve the keyspace name from a store URI.
     * The keyspace is taken from the path part of the URI and may be empty, if
     * the default keyspace {@value DuctileDBStore#DEFAULT_TITAN_KEYSPACE}
     * is to be used.
     * 
     * @param uri
     *            is the URI where the keyspace name is to be extracted from.
     * @return The name of the keyspace is returned as {@link String} .
     */
    public static String retrieveKeyspaceFromURI(URI uri) {
	String path = uri.getPath();
	if (path.startsWith("/")) {
	    path = path.substring(1);
	}
	if (path.endsWith("/")) {
	    path = path.substring(0, path.length() - 1);
	}
	String[] splits = path.split("/");
	if (splits.length > 1) {
	    throw new XOException("The URI for this store may only contain a single path entry for the keyspace.");
	}
	return splits[0];
    }

    /**
     * This field contains the whole titanGraph after connection to the
     * database.
     */
    private DuctileDBGraph graph = null;

    /**
     * This field contains the Cassandra host to connect to.
     */
    private final String host;
    /**
     * This field contains the port of Cassandra.
     */
    private final int port;
    /**
     * This is the name of the keyspace to use for Titan.
     */
    private final String keyspace;

    /**
     * This is the initial value constructor.
     * 
     * @param host
     *            is the host for Cassandra for Titan to connect to.
     * @param port
     *            is the port for Cassandra for Titan to connect to.
     * @param keyspace
     *            is
     */
    public DuctileDBStore(String host, int port, String keyspace) {
	if ((host == null) || (host.isEmpty())) {
	    throw new IllegalArgumentException("The host must not be null or empty.");
	}
	this.host = host;
	if (port <= 0) {
	    this.port = DEFAULT_CASSANDRA_THRIFT_PORT;
	} else {
	    this.port = port;
	}
	if ((keyspace == null) || (keyspace.isEmpty())) {
	    this.keyspace = DEFAULT_TITAN_KEYSPACE;
	} else {
	    this.keyspace = keyspace;
	}
    }

    /**
     * Returns the host name of the Cassandra server.
     * 
     * @return A {@link String} with the host name is returned.
     */
    public String getHost() {
	return host;
    }

    /**
     * Returns the port of the Cassandra server.
     * 
     * @return An <code>int</code> is returned with the port.
     */
    public int getPort() {
	return port;
    }

    /**
     * Returns the currently used keyspace.
     * 
     * @return A {@link String} is returned with the name of the keyspace.
     */
    public String getKeyspace() {
	return keyspace;
    }

    /**
     * This method returns the DuctileDBGraph object when database is connected.
     * 
     * @return A DuctileDBGraph is returned.
     */
    public final DuctileDBGraph getTitanGraph() {
	return graph;
    }

    @Override
    public DatastoreMetadataFactory<DuctileDBVertexMetadata, String, DuctileDBEdgeMetadata, String> getMetadataFactory() {
	return new TitanMetadataFactory();
    }

    @Override
    public void init(Map<Class<?>, TypeMetadata> registeredMetadata) {
	try {
	    logger.info("Initializing eXtended Objects for DuctileDB...");
	    Configuration configuration = new BaseConfiguration();
	    configuration.setProperty("storage.hostname", host);
	    if (port > 0) {
		configuration.setProperty("storage.port", port);
	    }
	    if (keyspace != null) {
		configuration.setProperty("storage.cassandra.keyspace", keyspace);
	    }
	    graph = GraphFactory.createGraph(configuration);
	} catch (IOException e) {
	    throw new XOException("Could not initialize eXtended Objects for DuctileDB.", e);
	}
    }

    @Override
    public DuctileDBStoreSession createSession() {
	return new DuctileDBStoreSession(graph);
    }

    @Override
    public void close() {
	logger.info("Shutting down eXtended Objects for Titan on Cassandra...");
	graph.shutdown();
	graph = null;
    }
}

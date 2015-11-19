package com.puresoltechnologies.ductiledb.xo.impl;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
 * This class implements an XO Datastore for DuctileDB.
 * </p>
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBStore
	implements Datastore<DuctileDBStoreSession, DuctileDBVertexMetadata, String, DuctileDBEdgeMetadata, String> {

    private static final Logger logger = LoggerFactory.getLogger(DuctileDBStore.class);

    /**
     * This constant contains the default name of the DuctileDB namespace which
     * is set to {@value #DEFAULT_DUCTILEDB_NAMESPACE}.
     */
    public static final String DEFAULT_DUCTILEDB_NAMESPACE = "ductiledb";

    /**
     * This is a helper method to retrieve the namespace name from a store URI.
     * The namespace is taken from the path part of the URI and may be empty, if
     * the default namespace {@value DuctileDBStore#DEFAULT_DUCTILEDB_NAMESPACE}
     * is to be used.
     * 
     * @param uri
     *            is the URI where the namespace name is to be extracted from.
     * @return The name of the namespace is returned as {@link String} .
     */
    public static String retrieveNamespaceFromURI(URI uri) {
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
     * This field contains the whole graph after connection to the database.
     */
    private DuctileDBGraph graph = null;

    /**
     * This field contains the path to hbase-site.xml to connect to HBase client
     * for DuctileDB.
     */
    private final URL hbaseSitePath;
    /**
     * This is the name of the namespace to use for DuctileDB.
     */
    private final String namespace;

    /**
     * This is the initial value constructor.
     * 
     * @param hbaseSitePath
     *            is the host for Cassandra for DuctileDB to connect to.
     * @param port
     *            is the port for Cassandra for DuctileDB to connect to.
     * @param namespace
     *            is
     */
    public DuctileDBStore(URL hbaseSitePath, String namespace) {
	if (hbaseSitePath == null) {
	    throw new IllegalArgumentException("The host must not be null or empty.");
	}
	this.hbaseSitePath = hbaseSitePath;
	if ((namespace == null) || (namespace.isEmpty())) {
	    this.namespace = DEFAULT_DUCTILEDB_NAMESPACE;
	} else {
	    this.namespace = namespace;
	}
    }

    /**
     * Returns the host name of the Cassandra server.
     * 
     * @return A {@link String} with the host name is returned.
     */
    public URL getHBaseSitePath() {
	return hbaseSitePath;
    }

    /**
     * Returns the currently used keyspace.
     * 
     * @return A {@link String} is returned with the name of the keyspace.
     */
    public String getKeyspace() {
	return namespace;
    }

    /**
     * This method returns the DuctileDBGraph object when database is connected.
     * 
     * @return A DuctileDBGraph is returned.
     */
    public final DuctileDBGraph getGraph() {
	return graph;
    }

    @Override
    public DatastoreMetadataFactory<DuctileDBVertexMetadata, String, DuctileDBEdgeMetadata, String> getMetadataFactory() {
	return new DuctileDBMetadataFactory();
    }

    @Override
    public void init(Map<Class<?>, TypeMetadata> registeredMetadata) {
	try {
	    logger.info("Initializing eXtended Objects for DuctileDB...");
	    Configuration hbaseConfiguration = HBaseConfiguration.create();
	    hbaseConfiguration.addResource(new Path(hbaseSitePath.getPath()));
	    graph = GraphFactory.createGraph(hbaseConfiguration);
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
	logger.info("Shutting down eXtended Objects for DuctileDB...");
	if (graph != null) {
	    graph.shutdown();
	    graph = null;
	}
    }
}

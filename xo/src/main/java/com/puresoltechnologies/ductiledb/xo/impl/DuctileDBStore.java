package com.puresoltechnologies.ductiledb.xo.impl;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.buschmais.xo.api.XOException;
import com.buschmais.xo.spi.datastore.Datastore;
import com.buschmais.xo.spi.datastore.DatastoreMetadataFactory;
import com.buschmais.xo.spi.metadata.type.TypeMetadata;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraph;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileEdgeMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileVertexMetadata;

/**
 * <p>
 * This class implements an XO Datastore for DuctileDB.
 * </p>
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBStore
	implements Datastore<DuctileStoreSession, DuctileVertexMetadata, String, DuctileEdgeMetadata, String> {

    private static final Logger logger = LoggerFactory.getLogger(DuctileDBStore.class);

    /**
     * This constant contains the default name of the DuctileDB namespace which
     * is set to {@value #DEFAULT_DUCTILEDB_NAMESPACE}.
     */
    public static final String DEFAULT_DUCTILEDB_NAMESPACE = "ductiledb";

    /**
     * This field contains the whole graph after connection to the database.
     */
    private DuctileGraph graph = null;

    /**
     * This field contains the path to hbase-site.xml to connect to HBase client
     * for DuctileDB.
     */
    private final File hbaseSitePath;
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
    public DuctileDBStore(File hbaseSitePath, String namespace) {
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
    public File getHBaseSitePath() {
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
    public final DuctileGraph getGraph() {
	return graph;
    }

    @Override
    public DatastoreMetadataFactory<DuctileVertexMetadata, String, DuctileEdgeMetadata, String> getMetadataFactory() {
	return new DuctileDBMetadataFactory();
    }

    @Override
    public void init(Map<Class<?>, TypeMetadata> registeredMetadata) {
	try {
	    logger.info("Initializing eXtended Objects for DuctileDB...");
	    BaseConfiguration configuration = new BaseConfiguration();
	    configuration.addProperty(Graph.GRAPH, DuctileGraph.class.getName());
	    graph = DuctileGraph.open(configuration);
	} catch (IOException e) {
	    throw new XOException("Could not initialize eXtended Objects for DuctileDB.", e);
	}
    }

    @Override
    public DuctileStoreSession createSession() {
	return new DuctileStoreSession(graph);
    }

    @Override
    public void close() {
	logger.info("Shutting down eXtended Objects for DuctileDB...");
	if (graph != null) {
	    try {
		graph.close();
	    } catch (Exception e) {
		throw new XOException("Could not close graph.", e);
	    }
	    graph = null;
	}
    }
}

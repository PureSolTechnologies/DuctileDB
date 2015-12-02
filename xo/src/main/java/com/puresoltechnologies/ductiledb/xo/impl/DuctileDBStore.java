package com.puresoltechnologies.ductiledb.xo.impl;

import java.io.File;
import java.io.IOException;
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
import com.puresoltechnologies.ductiledb.core.GraphFactory;
import com.puresoltechnologies.ductiledb.core.core.core.DuctileDBGraph;
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
     * This field contains the whole graph after connection to the database.
     */
    private DuctileDBGraph graph = null;

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

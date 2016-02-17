package com.puresoltechnologies.ductiledb.xo.impl;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.buschmais.xo.api.XOException;
import com.buschmais.xo.spi.datastore.Datastore;
import com.buschmais.xo.spi.datastore.DatastoreMetadataFactory;
import com.buschmais.xo.spi.metadata.method.IndexedPropertyMethodMetadata;
import com.buschmais.xo.spi.metadata.type.TypeMetadata;
import com.google.protobuf.ServiceException;
import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.ElementType;
import com.puresoltechnologies.ductiledb.api.schema.DuctileDBSchemaManager;
import com.puresoltechnologies.ductiledb.api.schema.PropertyDefinition;
import com.puresoltechnologies.ductiledb.api.schema.UniqueConstraint;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphFactory;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileEdge;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraph;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileVertex;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileEdgeMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileIndexedPropertyMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileVertexMetadata;

/**
 * <p>
 * This class implements an XO Datastore for DuctileDB.
 * </p>
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileStore
	implements Datastore<DuctileStoreSession, DuctileVertexMetadata, String, DuctileEdgeMetadata, String> {

    private static final Logger logger = LoggerFactory.getLogger(DuctileStore.class);

    /**
     * This constant contains the default name of the DuctileDB namespace which
     * is set to {@value #DEFAULT_DUCTILEDB_NAMESPACE}.
     */
    public static final String DEFAULT_DUCTILEDB_NAMESPACE = "ductiledb";

    /**
     * This field contains the connection to HBase. This {@link Connection}
     * object is thread-safe an can be reused for multiple session, because
     * opening a connection is quite expensive.
     */
    private Connection connection = null;

    /**
     * Contains the metadata factory for this XO implementation.
     */
    private final DuctileMetadataFactory ductileMetadataFactory = new DuctileMetadataFactory();

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
     *            is the path to the HBase site file.
     * @param namespace
     *            is the name of the namespace to connect to.
     */
    public DuctileStore(File hbaseSitePath, String namespace) {
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

    @Override
    public DatastoreMetadataFactory<DuctileVertexMetadata, String, DuctileEdgeMetadata, String> getMetadataFactory() {
	return ductileMetadataFactory;
    }

    @Override
    public void init(Map<Class<?>, TypeMetadata> registeredMetadata) {
	try {
	    logger.info("Initializing eXtended Objects for DuctileDB...");
	    connection = DuctileDBGraphFactory.createConnection(hbaseSitePath);
	    checkAndInitializePropertyIndizes(registeredMetadata);
	} catch (IOException | ServiceException e) {
	    throw new XOException("Could not initialize eXtended Objects for DuctileDB.", e);
	}
    }

    private void checkAndInitializePropertyIndizes(Map<Class<?>, TypeMetadata> registeredMetadata) {
	for (TypeMetadata metadata : registeredMetadata.values()) {
	    IndexedPropertyMethodMetadata<?> indexedProperty = metadata.getIndexedProperty();
	    if (indexedProperty != null) {
		DuctileIndexedPropertyMetadata datastoreMetadata = (DuctileIndexedPropertyMetadata) indexedProperty
			.getDatastoreMetadata();
		String name = datastoreMetadata.getName();
		Class<? extends Serializable> dataType = datastoreMetadata.getDataType();
		Class<? extends Element> type = datastoreMetadata.getType();
		boolean unique = datastoreMetadata.isUnique();
		logger.info("Indexed property '" + name + "' was found (unique=" + unique
			+ "). Check for presence of index...");
		ElementType elementType;
		if (DuctileVertex.class.isAssignableFrom(type)) {
		    elementType = ElementType.VERTEX;
		} else if (DuctileEdge.class.isAssignableFrom(type)) {
		    elementType = ElementType.EDGE;
		} else {
		    throw new XOException("Unsupported element type '" + type.getName() + "' found.");
		}
		checkAndCreatePropertyIndex(name, dataType, elementType, unique);
	    }
	}
    }

    private <T extends Serializable> void checkAndCreatePropertyIndex(String name, Class<T> dataType, ElementType type,
	    boolean unique) {
	try (DuctileDBGraph graph = DuctileDBGraphFactory.createGraph(connection)) {
	    DuctileDBSchemaManager schemaManager = graph.createSchemaManager();
	    PropertyDefinition<T> propertyDefinition = schemaManager.getPropertyDefinition(type, name);
	    if (propertyDefinition == null) {
		logger.info("Create index for property '" + name + "'.");
		propertyDefinition = new PropertyDefinition<T>(type, name, dataType,
			unique ? UniqueConstraint.TYPE : UniqueConstraint.NONE);
		schemaManager.defineProperty(propertyDefinition);
	    }
	} catch (IOException e) {
	    throw new XOException("Could not create property defintion.", e);
	}
    }

    @Override
    public DuctileStoreSession createSession() {
	try {
	    BaseConfiguration configuration = new BaseConfiguration();
	    configuration.setProperty(Graph.GRAPH, DuctileGraph.class.getName());
	    return new DuctileStoreSession(connection, configuration);
	} catch (IOException e) {
	    throw new XOException("Could not create graph.", e);
	}
    }

    @Override
    public void close() {
	logger.info("Shutting down eXtended Objects for DuctileDB...");
	if (connection != null) {
	    try {
		connection.close();
	    } catch (Exception e) {
		throw new XOException("Could not close graph.", e);
	    }
	    connection = null;
	}
    }
}

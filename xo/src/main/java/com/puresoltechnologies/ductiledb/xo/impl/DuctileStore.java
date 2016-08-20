package com.puresoltechnologies.ductiledb.xo.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.Map;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.buschmais.xo.api.XOException;
import com.buschmais.xo.spi.datastore.Datastore;
import com.buschmais.xo.spi.datastore.DatastoreMetadataFactory;
import com.buschmais.xo.spi.metadata.method.IndexedPropertyMethodMetadata;
import com.buschmais.xo.spi.metadata.type.TypeMetadata;
import com.puresoltechnologies.ductiledb.api.graph.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.graph.ElementType;
import com.puresoltechnologies.ductiledb.api.graph.schema.DuctileDBSchemaManager;
import com.puresoltechnologies.ductiledb.api.graph.schema.PropertyDefinition;
import com.puresoltechnologies.ductiledb.api.graph.schema.UniqueConstraint;
import com.puresoltechnologies.ductiledb.core.DuctileDBConfiguration;
import com.puresoltechnologies.ductiledb.core.DuctileDBFactory;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
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
     * This field contains the {@link DuctileDBGraph}. This object is
     * thread-safe an can be reused for multiple session, because opening a new
     * graph is quite expensive.
     */
    private DuctileDBGraph graph = null;

    /**
     * Contains the metadata factory for this XO implementation.
     */
    private final DuctileMetadataFactory ductileMetadataFactory = new DuctileMetadataFactory();

    /**
     * This field contains the path to hbase-site.xml to connect to HBase client
     * for DuctileDB.
     */
    private final URL ductileDBConfigFile;
    /**
     * This is the name of the namespace to use for DuctileDB.
     */
    private final String namespace;

    /**
     * This is the initial value constructor.
     * 
     * @param ductileDBConfigFile
     *            is the path to the HBase site file.
     * @param namespace
     *            is the name of the namespace to connect to.
     * @throws IOException
     */
    public DuctileStore(URL ductileDBConfigFile) throws IOException {
	if (ductileDBConfigFile == null) {
	    throw new IllegalArgumentException("The config file URL must not be null or empty.");
	}
	this.ductileDBConfigFile = ductileDBConfigFile;
	DuctileDBConfiguration configuration;
	try (InputStream configStream = ductileDBConfigFile.openStream()) {
	    configuration = DuctileDBFactory.readConfiguration(configStream);
	}
	this.namespace = configuration.getGraph().getNamespace();
    }

    /**
     * Returns the host name of the HBase site file.
     * 
     * @return A {@link String} with the host name is returned.
     */
    public URL getDuctileDBConfigFile() {
	return ductileDBConfigFile;
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
	    graph = DuctileDBFactory.connect(ductileDBConfigFile).getGraph();
	    checkAndInitializePropertyIndizes(registeredMetadata);
	} catch (IOException | StorageException | SchemaException e) {
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
	DuctileDBSchemaManager schemaManager = graph.createSchemaManager();
	PropertyDefinition<T> propertyDefinition = schemaManager.getPropertyDefinition(type, name);
	if (propertyDefinition == null) {
	    logger.info("Create index for property '" + name + "'.");
	    propertyDefinition = new PropertyDefinition<>(type, name, dataType,
		    unique ? UniqueConstraint.TYPE : UniqueConstraint.NONE);
	    schemaManager.defineProperty(propertyDefinition);
	}
    }

    @Override
    public DuctileStoreSession createSession() {
	try {
	    BaseConfiguration configuration = new BaseConfiguration();
	    configuration.setProperty(Graph.GRAPH, DuctileGraph.class.getName());
	    return new DuctileStoreSession(graph, configuration);
	} catch (IOException e) {
	    throw new XOException("Could not create graph.", e);
	}
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

package com.puresoltechnologies.ductiledb.core.graph.schema;

import java.io.Serializable;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.api.graph.ElementType;
import com.puresoltechnologies.ductiledb.api.graph.manager.DuctileDBGraphManagerException;
import com.puresoltechnologies.ductiledb.api.graph.manager.DuctileDBSchemaManagerException;
import com.puresoltechnologies.ductiledb.api.graph.schema.DuctileDBPropertyAlreadyDefinedException;
import com.puresoltechnologies.ductiledb.api.graph.schema.DuctileDBSchemaException;
import com.puresoltechnologies.ductiledb.api.graph.schema.DuctileDBSchemaManager;
import com.puresoltechnologies.ductiledb.api.graph.schema.DuctileDBTypeAlreadyDefinedException;
import com.puresoltechnologies.ductiledb.api.graph.schema.PropertyDefinition;
import com.puresoltechnologies.ductiledb.api.graph.schema.UniqueConstraint;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.storage.engine.Delete;
import com.puresoltechnologies.ductiledb.storage.engine.Get;
import com.puresoltechnologies.ductiledb.storage.engine.Put;
import com.puresoltechnologies.ductiledb.storage.engine.Result;
import com.puresoltechnologies.ductiledb.storage.engine.ResultScanner;
import com.puresoltechnologies.ductiledb.storage.engine.Scan;
import com.puresoltechnologies.ductiledb.storage.engine.Table;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;

public class DuctileDBSchemaManagerImpl implements DuctileDBSchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(DuctileDBSchemaManagerImpl.class);

    private final DuctileDBGraphImpl graph;
    private final NamespaceDescriptor namespace;
    private final TableDescriptor propertyDefinitionsTable;

    public DuctileDBSchemaManagerImpl(DuctileDBGraphImpl graph) {
	super();
	this.graph = graph;
	DatabaseEngine storageEngine = graph.getStorageEngine();
	SchemaManager schemaManager = storageEngine.getSchemaManager();
	namespace = schemaManager.getNamespace(GraphSchema.DUCTILEDB_NAMESPACE);
	propertyDefinitionsTable = namespace.getTable(DatabaseTable.PROPERTY_DEFINITIONS.getName());
    }

    @Override
    public Iterable<String> getDefinedProperties() {
	Table table = graph.getStorageEngine().getTable(GraphSchema.DUCTILEDB_NAMESPACE,
		DatabaseTable.PROPERTY_DEFINITIONS.getName());
	ResultScanner scanner = table.getScanner(new Scan());
	Set<String> propertyNames = new HashSet<>();
	scanner.forEach((result) -> propertyNames.add(Bytes.toString(result.getRowKey())));
	return propertyNames;
    }

    @Override
    public <T extends Serializable> void defineProperty(PropertyDefinition<T> definition) {
	if (definition == null) {
	    throw new IllegalArgumentException("definition must no be null.");
	}
	if (getPropertyDefinition(definition.getElementType(), definition.getPropertyKey()) != null) {
	    throw new DuctileDBPropertyAlreadyDefinedException(definition);
	}

	Table table = graph.getStorageEngine().getTable(GraphSchema.DUCTILEDB_NAMESPACE,
		DatabaseTable.PROPERTY_DEFINITIONS.getName());
	Put put = new Put(Bytes.toBytes(definition.getPropertyKey()));
	switch (definition.getElementType()) {
	case VERTEX:
	    put.addColumn(DatabaseColumnFamily.VERTEX_DEFINITION.getNameBytes(), GraphSchema.PROPERTY_TYPE_COLUMN_BYTES,
		    Bytes.toBytes(definition.getPropertyType().getName()));
	    put.addColumn(DatabaseColumnFamily.VERTEX_DEFINITION.getNameBytes(), GraphSchema.ELEMENT_TYPE_COLUMN_BYTES,
		    Bytes.toBytes(definition.getElementType().name()));
	    put.addColumn(DatabaseColumnFamily.VERTEX_DEFINITION.getNameBytes(), GraphSchema.UNIQUENESS_COLUMN_BYTES,
		    Bytes.toBytes(definition.getUniqueConstraint().name()));
	    break;
	case EDGE:
	    put.addColumn(DatabaseColumnFamily.EDGE_DEFINITION.getNameBytes(), GraphSchema.PROPERTY_TYPE_COLUMN_BYTES,
		    Bytes.toBytes(definition.getPropertyType().getName()));
	    put.addColumn(DatabaseColumnFamily.EDGE_DEFINITION.getNameBytes(), GraphSchema.ELEMENT_TYPE_COLUMN_BYTES,
		    Bytes.toBytes(definition.getElementType().name()));
	    put.addColumn(DatabaseColumnFamily.EDGE_DEFINITION.getNameBytes(), GraphSchema.UNIQUENESS_COLUMN_BYTES,
		    Bytes.toBytes(definition.getUniqueConstraint().name()));
	    break;
	default:
	    throw new DuctileDBSchemaManagerException(
		    "Cannot define property for element '" + definition.getElementType() + "'.");
	}
	try {
	    table.put(put);
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not define property.", e);
	}
	graph.getSchema().defineProperty(definition);
    }

    @Override
    public <T extends Serializable> PropertyDefinition<T> getPropertyDefinition(ElementType elementType,
	    String propertyKey) {
	try {
	    Table table = graph.getStorageEngine().getTable(GraphSchema.DUCTILEDB_NAMESPACE,
		    DatabaseTable.PROPERTY_DEFINITIONS.getName());
	    Get get = new Get(Bytes.toBytes(propertyKey));
	    byte[] columnFamily = null;
	    switch (elementType) {
	    case VERTEX:
		columnFamily = DatabaseColumnFamily.VERTEX_DEFINITION.getNameBytes();
		break;
	    case EDGE:
		columnFamily = DatabaseColumnFamily.EDGE_DEFINITION.getNameBytes();
		break;
	    default:
		throw new DuctileDBSchemaManagerException("Cannot read property for element '" + elementType + "'.");
	    }
	    get.addFamily(columnFamily);
	    Result result = table.get(get);
	    if (result == null) {
		return null;
	    }
	    NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(columnFamily);
	    if (familyMap == null) {
		return null;
	    }
	    @SuppressWarnings("unchecked")
	    Class<T> type = (Class<T>) Class
		    .forName(Bytes.toString(familyMap.get(GraphSchema.PROPERTY_TYPE_COLUMN_BYTES)));
	    UniqueConstraint unique = UniqueConstraint
		    .valueOf(Bytes.toString(familyMap.get(GraphSchema.UNIQUENESS_COLUMN_BYTES)));
	    PropertyDefinition<T> definition = new PropertyDefinition<>(elementType, propertyKey, type, unique);
	    return definition;
	} catch (ClassNotFoundException e) {
	    throw new DuctileDBSchemaManagerException("Could not read property names.", e);
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not read property definition.", e);
	}
    }

    @Override
    public void removePropertyDefinition(ElementType elementType, String propertyKey) {
	Table table = graph.getStorageEngine().getTable(GraphSchema.DUCTILEDB_NAMESPACE,
		DatabaseTable.PROPERTY_DEFINITIONS.getName());
	Delete delete = new Delete(Bytes.toBytes(propertyKey));
	switch (elementType) {
	case VERTEX:
	    delete.addFamily(DatabaseColumnFamily.VERTEX_DEFINITION.getNameBytes());
	    break;
	case EDGE:
	    delete.addFamily(DatabaseColumnFamily.EDGE_DEFINITION.getNameBytes());
	    break;
	default:
	    throw new DuctileDBSchemaManagerException(
		    "Could not delete property definition for element '" + elementType + "'.");
	}
	try {
	    table.delete(delete);
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not remove property.", e);
	}
	graph.getSchema().removeProperty(elementType, propertyKey);
    }

    @Override
    public Iterable<String> getDefinedTypes() {
	Table table = graph.getStorageEngine().getTable(GraphSchema.DUCTILEDB_NAMESPACE,
		DatabaseTable.TYPE_DEFINITIONS.getName());
	ResultScanner scanner = table.getScanner(new Scan());
	Set<String> typeNames = new HashSet<>();
	scanner.forEach((result) -> typeNames.add(Bytes.toString(result.getRowKey())));
	return typeNames;
    }

    @Override
    public void defineType(ElementType elementType, String typeName, Set<String> propertyKeys) {
	if (elementType == null) {
	    throw new IllegalArgumentException("elementType must not be null.");
	}
	if ((typeName == null) || (typeName.isEmpty())) {
	    throw new IllegalArgumentException("typeName must not be null or empty.");
	}
	if ((propertyKeys == null) || (propertyKeys.isEmpty())) {
	    throw new IllegalArgumentException("propertyKeys must not be null or empty.");
	}
	if (getTypeDefinition(elementType, typeName) != null) {
	    throw new DuctileDBTypeAlreadyDefinedException(typeName);
	}
	for (String propertyKey : propertyKeys) {
	    if (getPropertyDefinition(elementType, propertyKey) == null) {
		throw new DuctileDBSchemaException("Cannot define type '" + typeName
			+ "' with unknown property definition for key  '" + propertyKey + "'.");
	    }
	}
	DatabaseEngine storageEngine = graph.getStorageEngine();
	Table table = storageEngine.getTable(GraphSchema.DUCTILEDB_NAMESPACE, DatabaseTable.TYPE_DEFINITIONS.getName());
	Put put = new Put(Bytes.toBytes(typeName));
	switch (elementType) {
	case VERTEX:
	    for (String propertyKey : propertyKeys) {
		put.addColumn(DatabaseColumnFamily.VERTEX_DEFINITION.getNameBytes(), Bytes.toBytes(propertyKey),
			Bytes.empty());
	    }
	    break;
	case EDGE:
	    for (String propertyKey : propertyKeys) {
		put.addColumn(DatabaseColumnFamily.EDGE_DEFINITION.getNameBytes(), Bytes.toBytes(propertyKey),
			Bytes.empty());
	    }
	    break;
	default:
	    throw new DuctileDBSchemaManagerException("Cannot define type for element '" + elementType + "'.");
	}
	try {
	    table.put(put);
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not define type.", e);
	}
	graph.getSchema().defineType(elementType, typeName, propertyKeys);
    }

    @Override
    public Set<String> getTypeDefinition(ElementType elementType, String typeName) {
	Table table = graph.getStorageEngine().getTable(GraphSchema.DUCTILEDB_NAMESPACE,
		DatabaseTable.TYPE_DEFINITIONS.getName());
	Get get = new Get(Bytes.toBytes(typeName));
	byte[] columnFamily = null;
	switch (elementType) {
	case VERTEX:
	    columnFamily = DatabaseColumnFamily.VERTEX_DEFINITION.getNameBytes();
	    break;
	case EDGE:
	    columnFamily = DatabaseColumnFamily.EDGE_DEFINITION.getNameBytes();
	    break;
	default:
	    throw new DuctileDBSchemaManagerException("Cannot read type for element '" + elementType + "'.");
	}
	get.addFamily(columnFamily);
	Result result;
	try {
	    result = table.get(get);
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not read type definition.", e);
	}
	if (result == null) {
	    return null;
	}
	NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(columnFamily);
	if (familyMap == null) {
	    return null;
	}
	Set<String> propertyKeys = new HashSet<>();
	for (byte[] propertyKeyBytes : familyMap.keySet()) {
	    propertyKeys.add(Bytes.toString(propertyKeyBytes));
	}
	return propertyKeys;
    }

    @Override
    public void removeTypeDefinition(ElementType elementType, String typeName) {
	DatabaseEngine storageEngine = graph.getStorageEngine();
	Table table = storageEngine.getTable(GraphSchema.DUCTILEDB_NAMESPACE, DatabaseTable.TYPE_DEFINITIONS.getName());
	Delete delete = new Delete(Bytes.toBytes(typeName));
	switch (elementType) {
	case VERTEX:
	    delete.addFamily(DatabaseColumnFamily.VERTEX_DEFINITION.getNameBytes());
	    break;
	case EDGE:
	    delete.addFamily(DatabaseColumnFamily.EDGE_DEFINITION.getNameBytes());
	    break;
	default:
	    throw new DuctileDBSchemaManagerException(
		    "Could not delete type definition for element '" + elementType + "'.");
	}
	try {
	    table.delete(delete);
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not remove type definition.", e);
	}
	graph.getSchema().removeType(elementType, typeName);
    }

}

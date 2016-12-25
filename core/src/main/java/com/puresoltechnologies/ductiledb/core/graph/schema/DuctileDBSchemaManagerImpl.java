package com.puresoltechnologies.ductiledb.core.graph.schema;

import java.io.Serializable;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;

import com.puresoltechnologies.ductiledb.core.graph.ElementType;
import com.puresoltechnologies.ductiledb.core.graph.GraphStoreImpl;
import com.puresoltechnologies.ductiledb.core.graph.manager.DuctileDBGraphManagerException;
import com.puresoltechnologies.ductiledb.core.graph.manager.DuctileDBSchemaManagerException;
import com.puresoltechnologies.ductiledb.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.engine.Delete;
import com.puresoltechnologies.ductiledb.engine.Get;
import com.puresoltechnologies.ductiledb.engine.Put;
import com.puresoltechnologies.ductiledb.engine.Result;
import com.puresoltechnologies.ductiledb.engine.ResultScanner;
import com.puresoltechnologies.ductiledb.engine.Scan;
import com.puresoltechnologies.ductiledb.engine.TableEngine;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnValue;
import com.puresoltechnologies.ductiledb.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.engine.schema.TableDescriptor;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;

public class DuctileDBSchemaManagerImpl implements DuctileDBSchemaManager {

    private final GraphStoreImpl graph;
    private final String namespace;
    private final NamespaceDescriptor namespaceDescriptor;
    private final TableDescriptor propertyDefinitionsTable;

    public DuctileDBSchemaManagerImpl(GraphStoreImpl graph) {
	super();
	this.graph = graph;
	this.namespace = graph.getConfiguration().getNamespace();
	DatabaseEngine storageEngine = graph.getStorageEngine();
	SchemaManager schemaManager = storageEngine.getSchemaManager();
	namespaceDescriptor = schemaManager.getNamespace(namespace);
	propertyDefinitionsTable = namespaceDescriptor.getTable(DatabaseTable.PROPERTY_DEFINITIONS.getName());
    }

    @Override
    public Iterable<String> getDefinedProperties() {
	TableEngine table = graph.getStorageEngine().getTable(namespace, DatabaseTable.PROPERTY_DEFINITIONS.getName());
	ResultScanner scanner = table.getScanner(new Scan());
	Set<String> propertyNames = new HashSet<>();
	scanner.forEach((result) -> propertyNames.add(result.getRowKey().toString()));
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

	TableEngine table = graph.getStorageEngine().getTable(namespace, DatabaseTable.PROPERTY_DEFINITIONS.getName());
	Put put = new Put(Key.of(definition.getPropertyKey()));
	switch (definition.getElementType()) {
	case VERTEX:
	    put.addColumn(DatabaseColumnFamily.VERTEX_DEFINITION.getKey(), GraphSchema.PROPERTY_TYPE_KEY,
		    ColumnValue.of(definition.getPropertyType().getName()));
	    put.addColumn(DatabaseColumnFamily.VERTEX_DEFINITION.getKey(), GraphSchema.ELEMENT_TYPE_COLUMN_KEY,
		    ColumnValue.of(definition.getElementType().name()));
	    put.addColumn(DatabaseColumnFamily.VERTEX_DEFINITION.getKey(), GraphSchema.UNIQUENESS_COLUMN_KEY,
		    ColumnValue.of(definition.getUniqueConstraint().name()));
	    break;
	case EDGE:
	    put.addColumn(DatabaseColumnFamily.EDGE_DEFINITION.getKey(), GraphSchema.PROPERTY_TYPE_KEY,
		    ColumnValue.of(definition.getPropertyType().getName()));
	    put.addColumn(DatabaseColumnFamily.EDGE_DEFINITION.getKey(), GraphSchema.ELEMENT_TYPE_COLUMN_KEY,
		    ColumnValue.of(definition.getElementType().name()));
	    put.addColumn(DatabaseColumnFamily.EDGE_DEFINITION.getKey(), GraphSchema.UNIQUENESS_COLUMN_KEY,
		    ColumnValue.of(definition.getUniqueConstraint().name()));
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
	    TableEngine table = graph.getStorageEngine().getTable(namespace,
		    DatabaseTable.PROPERTY_DEFINITIONS.getName());
	    Get get = new Get(Key.of(propertyKey));
	    Key columnFamily = null;
	    switch (elementType) {
	    case VERTEX:
		columnFamily = DatabaseColumnFamily.VERTEX_DEFINITION.getKey();
		break;
	    case EDGE:
		columnFamily = DatabaseColumnFamily.EDGE_DEFINITION.getKey();
		break;
	    default:
		throw new DuctileDBSchemaManagerException("Cannot read property for element '" + elementType + "'.");
	    }
	    get.addFamily(columnFamily);
	    Result result = table.get(get);
	    if (result == null) {
		return null;
	    }
	    NavigableMap<Key, ColumnValue> familyMap = result.getFamilyMap(columnFamily);
	    if (familyMap == null) {
		return null;
	    }
	    @SuppressWarnings("unchecked")
	    Class<T> type = (Class<T>) Class.forName(familyMap.get(GraphSchema.PROPERTY_TYPE_KEY).toString());
	    UniqueConstraint unique = UniqueConstraint
		    .valueOf(familyMap.get(GraphSchema.UNIQUENESS_COLUMN_KEY).toString());
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
	TableEngine table = graph.getStorageEngine().getTable(namespace, DatabaseTable.PROPERTY_DEFINITIONS.getName());
	Delete delete = new Delete(Key.of(propertyKey));
	switch (elementType) {
	case VERTEX:
	    delete.addFamily(DatabaseColumnFamily.VERTEX_DEFINITION.getKey());
	    break;
	case EDGE:
	    delete.addFamily(DatabaseColumnFamily.EDGE_DEFINITION.getKey());
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
	TableEngine table = graph.getStorageEngine().getTable(namespace, DatabaseTable.TYPE_DEFINITIONS.getName());
	ResultScanner scanner = table.getScanner(new Scan());
	Set<String> typeNames = new HashSet<>();
	scanner.forEach((result) -> typeNames.add(result.getRowKey().toString()));
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
	TableEngine table = storageEngine.getTable(namespace, DatabaseTable.TYPE_DEFINITIONS.getName());
	Put put = new Put(Key.of(typeName));
	switch (elementType) {
	case VERTEX:
	    for (String propertyKey : propertyKeys) {
		put.addColumn(DatabaseColumnFamily.VERTEX_DEFINITION.getKey(), Key.of(propertyKey),
			ColumnValue.empty());
	    }
	    break;
	case EDGE:
	    for (String propertyKey : propertyKeys) {
		put.addColumn(DatabaseColumnFamily.EDGE_DEFINITION.getKey(), Key.of(propertyKey), ColumnValue.empty());
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
	TableEngine table = graph.getStorageEngine().getTable(namespace, DatabaseTable.TYPE_DEFINITIONS.getName());
	Get get = new Get(Key.of(typeName));
	Key columnFamily = null;
	switch (elementType) {
	case VERTEX:
	    columnFamily = DatabaseColumnFamily.VERTEX_DEFINITION.getKey();
	    break;
	case EDGE:
	    columnFamily = DatabaseColumnFamily.EDGE_DEFINITION.getKey();
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
	NavigableMap<Key, ColumnValue> familyMap = result.getFamilyMap(columnFamily);
	if (familyMap == null) {
	    return null;
	}
	Set<String> propertyKeys = new HashSet<>();
	for (Key propertyKey : familyMap.keySet()) {
	    propertyKeys.add(propertyKey.toString());
	}
	return propertyKeys;
    }

    @Override
    public void removeTypeDefinition(ElementType elementType, String typeName) {
	DatabaseEngine storageEngine = graph.getStorageEngine();
	TableEngine table = storageEngine.getTable(namespace, DatabaseTable.TYPE_DEFINITIONS.getName());
	Delete delete = new Delete(Key.of(typeName));
	switch (elementType) {
	case VERTEX:
	    delete.addFamily(DatabaseColumnFamily.VERTEX_DEFINITION.getKey());
	    break;
	case EDGE:
	    delete.addFamily(DatabaseColumnFamily.EDGE_DEFINITION.getKey());
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

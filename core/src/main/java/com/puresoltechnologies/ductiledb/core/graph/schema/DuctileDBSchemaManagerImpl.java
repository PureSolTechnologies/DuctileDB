package com.puresoltechnologies.ductiledb.core.graph.schema;

import java.io.Serializable;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.api.graph.ElementType;
import com.puresoltechnologies.ductiledb.api.graph.manager.DuctileDBSchemaManagerException;
import com.puresoltechnologies.ductiledb.api.graph.schema.DuctileDBPropertyAlreadyDefinedException;
import com.puresoltechnologies.ductiledb.api.graph.schema.DuctileDBSchemaException;
import com.puresoltechnologies.ductiledb.api.graph.schema.DuctileDBSchemaManager;
import com.puresoltechnologies.ductiledb.api.graph.schema.DuctileDBTypeAlreadyDefinedException;
import com.puresoltechnologies.ductiledb.api.graph.schema.PropertyDefinition;
import com.puresoltechnologies.ductiledb.api.graph.schema.UniqueConstraint;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.storage.engine.Delete;
import com.puresoltechnologies.ductiledb.storage.engine.Get;
import com.puresoltechnologies.ductiledb.storage.engine.Put;
import com.puresoltechnologies.ductiledb.storage.engine.Result;
import com.puresoltechnologies.ductiledb.storage.engine.ResultScanner;
import com.puresoltechnologies.ductiledb.storage.engine.Scan;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.storage.engine.Table;
import com.puresoltechnologies.ductiledb.storage.engine.utils.Bytes;

public class DuctileDBSchemaManagerImpl implements DuctileDBSchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(DuctileDBSchemaManagerImpl.class);

    private final DuctileDBGraphImpl graph;

    public DuctileDBSchemaManagerImpl(DuctileDBGraphImpl graph) {
	super();
	this.graph = graph;
    }

    @Override
    public Iterable<String> getDefinedProperties() {
	try (Table table = graph.getStorageEngine().getTable(HBaseTable.PROPERTY_DEFINITIONS.getName())) {
	    ResultScanner scanner = table.getScanner(new Scan());
	    Set<String> propertyNames = new HashSet<>();
	    scanner.forEach((result) -> propertyNames.add(Bytes.toString(result.getRow())));
	    return propertyNames;
	}
    }

    @Override
    public <T extends Serializable> void defineProperty(PropertyDefinition<T> definition) {
	if (definition == null) {
	    throw new IllegalArgumentException("definition must no be null.");
	}
	if (getPropertyDefinition(definition.getElementType(), definition.getPropertyKey()) != null) {
	    throw new DuctileDBPropertyAlreadyDefinedException(definition);
	}

	try (Table table = graph.getStorageEngine().getTable(HBaseTable.PROPERTY_DEFINITIONS.getName())) {
	    Put put = new Put(Bytes.toBytes(definition.getPropertyKey()));
	    switch (definition.getElementType()) {
	    case VERTEX:
		put.addColumn(HBaseColumnFamily.VERTEX_DEFINITION.getName(), GraphSchema.PROPERTY_TYPE_COLUMN_BYTES,
			Bytes.toBytes(definition.getPropertyType().getName()));
		put.addColumn(HBaseColumnFamily.VERTEX_DEFINITION.getName(), GraphSchema.ELEMENT_TYPE_COLUMN_BYTES,
			Bytes.toBytes(definition.getElementType().name()));
		put.addColumn(HBaseColumnFamily.VERTEX_DEFINITION.getName(), GraphSchema.UNIQUENESS_COLUMN_BYTES,
			Bytes.toBytes(definition.getUniqueConstraint().name()));
		break;
	    case EDGE:
		put.addColumn(HBaseColumnFamily.EDGE_DEFINITION.getName(), GraphSchema.PROPERTY_TYPE_COLUMN_BYTES,
			Bytes.toBytes(definition.getPropertyType().getName()));
		put.addColumn(HBaseColumnFamily.EDGE_DEFINITION.getName(), GraphSchema.ELEMENT_TYPE_COLUMN_BYTES,
			Bytes.toBytes(definition.getElementType().name()));
		put.addColumn(HBaseColumnFamily.EDGE_DEFINITION.getName(), GraphSchema.UNIQUENESS_COLUMN_BYTES,
			Bytes.toBytes(definition.getUniqueConstraint().name()));
		break;
	    default:
		throw new DuctileDBSchemaManagerException(
			"Cannot define property for element '" + definition.getElementType() + "'.");
	    }
	    table.put(put);
	    graph.getSchema().defineProperty(definition);
	}
    }

    @Override
    public <T extends Serializable> PropertyDefinition<T> getPropertyDefinition(ElementType elementType,
	    String propertyKey) {
	try (Table table = graph.getStorageEngine().getTable(HBaseTable.PROPERTY_DEFINITIONS.getName())) {
	    Get get = new Get(Bytes.toBytes(propertyKey));
	    String columnFamily = null;
	    switch (elementType) {
	    case VERTEX:
		columnFamily = HBaseColumnFamily.VERTEX_DEFINITION.getName();
		break;
	    case EDGE:
		columnFamily = HBaseColumnFamily.EDGE_DEFINITION.getName();
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
	}
    }

    @Override
    public void removePropertyDefinition(ElementType elementType, String propertyKey) {
	try (Table table = graph.getStorageEngine().getTable(HBaseTable.PROPERTY_DEFINITIONS.getName())) {
	    Delete delete = new Delete(Bytes.toBytes(propertyKey));
	    switch (elementType) {
	    case VERTEX:
		delete.addFamily(HBaseColumnFamily.VERTEX_DEFINITION.getName());
		break;
	    case EDGE:
		delete.addFamily(HBaseColumnFamily.EDGE_DEFINITION.getName());
		break;
	    default:
		throw new DuctileDBSchemaManagerException(
			"Could not delete property definition for element '" + elementType + "'.");
	    }
	    table.delete(delete);
	    graph.getSchema().removeProperty(elementType, propertyKey);
	}
    }

    @Override
    public Iterable<String> getDefinedTypes() {
	try (Table table = graph.getStorageEngine().getTable(HBaseTable.TYPE_DEFINITIONS.getName())) {
	    ResultScanner scanner = table.getScanner(new Scan());
	    Set<String> typeNames = new HashSet<>();
	    scanner.forEach((result) -> typeNames.add(Bytes.toString(result.getRow())));
	    return typeNames;
	}
    }

    @Override
    public void defineType(ElementType elementType, String typeName, Set<String> propertyKeys) {
	if (elementType == null) {
	    throw new IllegalArgumentException("elementType must not be null.");
	}
	if (typeName == null) {
	    throw new IllegalArgumentException("typeName must not be null.");
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
	try (Table table = storageEngine.getTable(HBaseTable.TYPE_DEFINITIONS.getName())) {
	    Put put = new Put(Bytes.toBytes(typeName));
	    switch (elementType) {
	    case VERTEX:
		for (String propertyKey : propertyKeys) {
		    put.addColumn(HBaseColumnFamily.VERTEX_DEFINITION.getName(), Bytes.toBytes(propertyKey),
			    Bytes.empty());
		}
		break;
	    case EDGE:
		for (String propertyKey : propertyKeys) {
		    put.addColumn(HBaseColumnFamily.EDGE_DEFINITION.getName(), Bytes.toBytes(propertyKey),
			    Bytes.empty());
		}
		break;
	    default:
		throw new DuctileDBSchemaManagerException("Cannot define type for element '" + elementType + "'.");
	    }
	    table.put(put);
	    graph.getSchema().defineType(elementType, typeName, propertyKeys);
	}
    }

    @Override
    public Set<String> getTypeDefinition(ElementType elementType, String typeName) {
	try (Table table = graph.getStorageEngine().getTable(HBaseTable.TYPE_DEFINITIONS.getName())) {
	    Get get = new Get(Bytes.toBytes(typeName));
	    String columnFamily = null;
	    switch (elementType) {
	    case VERTEX:
		columnFamily = HBaseColumnFamily.VERTEX_DEFINITION.getName();
		break;
	    case EDGE:
		columnFamily = HBaseColumnFamily.EDGE_DEFINITION.getName();
		break;
	    default:
		throw new DuctileDBSchemaManagerException("Cannot read type for element '" + elementType + "'.");
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
	    Set<String> propertyKeys = new HashSet<>();
	    for (byte[] propertyKeyBytes : familyMap.keySet()) {
		propertyKeys.add(Bytes.toString(propertyKeyBytes));
	    }
	    return propertyKeys;
	}
    }

    @Override
    public void removeTypeDefinition(ElementType elementType, String typeName) {
	DatabaseEngine storageEngine = graph.getStorageEngine();
	try (Table table = storageEngine.getTable(HBaseTable.TYPE_DEFINITIONS.getName())) {
	    Delete delete = new Delete(Bytes.toBytes(typeName));
	    switch (elementType) {
	    case VERTEX:
		delete.addFamily(HBaseColumnFamily.VERTEX_DEFINITION.getName());
		break;
	    case EDGE:
		delete.addFamily(HBaseColumnFamily.EDGE_DEFINITION.getName());
		break;
	    default:
		throw new DuctileDBSchemaManagerException(
			"Could not delete type definition for element '" + elementType + "'.");
	    }
	    table.delete(delete);
	    graph.getSchema().removeType(elementType, typeName);
	}
    }

}

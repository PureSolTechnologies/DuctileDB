package com.puresoltechnologies.ductiledb.core.graph.schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
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

public class DuctileDBSchemaManagerImpl implements DuctileDBSchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(DuctileDBSchemaManagerImpl.class);

    private final DuctileDBGraphImpl graph;

    public DuctileDBSchemaManagerImpl(DuctileDBGraphImpl graph) {
	super();
	this.graph = graph;
    }

    @Override
    public Iterable<String> getDefinedProperties() {
	try (Table table = graph.getConnection().getTable(HBaseTable.PROPERTY_DEFINITIONS.getTableName())) {
	    ResultScanner scanner = table.getScanner(new Scan());
	    Set<String> propertyNames = new HashSet<>();
	    scanner.forEach((result) -> propertyNames.add(Bytes.toString(result.getRow())));
	    return propertyNames;
	} catch (IOException e) {
	    throw new DuctileDBSchemaManagerException("Could not read property names.", e);
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
	try (Table table = graph.getConnection().getTable(HBaseTable.PROPERTY_DEFINITIONS.getTableName())) {
	    Put put = new Put(Bytes.toBytes(definition.getPropertyKey()));
	    switch (definition.getElementType()) {
	    case VERTEX:
		put.addColumn(HBaseColumnFamily.VERTEX_DEFINITION.getNameBytes(),
			HBaseSchema.PROPERTY_TYPE_COLUMN_BYTES, Bytes.toBytes(definition.getPropertyType().getName()));
		put.addColumn(HBaseColumnFamily.VERTEX_DEFINITION.getNameBytes(), HBaseSchema.ELEMENT_TYPE_COLUMN_BYTES,
			Bytes.toBytes(definition.getElementType().name()));
		put.addColumn(HBaseColumnFamily.VERTEX_DEFINITION.getNameBytes(), HBaseSchema.UNIQUENESS_COLUMN_BYTES,
			Bytes.toBytes(definition.getUniqueConstraint().name()));
		break;
	    case EDGE:
		put.addColumn(HBaseColumnFamily.EDGE_DEFINITION.getNameBytes(), HBaseSchema.PROPERTY_TYPE_COLUMN_BYTES,
			Bytes.toBytes(definition.getPropertyType().getName()));
		put.addColumn(HBaseColumnFamily.EDGE_DEFINITION.getNameBytes(), HBaseSchema.ELEMENT_TYPE_COLUMN_BYTES,
			Bytes.toBytes(definition.getElementType().name()));
		put.addColumn(HBaseColumnFamily.EDGE_DEFINITION.getNameBytes(), HBaseSchema.UNIQUENESS_COLUMN_BYTES,
			Bytes.toBytes(definition.getUniqueConstraint().name()));
		break;
	    default:
		throw new DuctileDBSchemaManagerException(
			"Cannot define property for element '" + definition.getElementType() + "'.");
	    }
	    table.put(put);
	    graph.getSchema().defineProperty(definition);
	} catch (IOException e) {
	    throw new DuctileDBSchemaManagerException("Could not read property names.", e);
	}
    }

    @Override
    public <T extends Serializable> PropertyDefinition<T> getPropertyDefinition(ElementType elementType,
	    String propertyKey) {
	try (Table table = graph.getConnection().getTable(HBaseTable.PROPERTY_DEFINITIONS.getTableName())) {
	    Get get = new Get(Bytes.toBytes(propertyKey));
	    byte[] columnFamily = null;
	    switch (elementType) {
	    case VERTEX:
		columnFamily = HBaseColumnFamily.VERTEX_DEFINITION.getNameBytes();
		break;
	    case EDGE:
		columnFamily = HBaseColumnFamily.EDGE_DEFINITION.getNameBytes();
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
		    .forName(Bytes.toString(familyMap.get(HBaseSchema.PROPERTY_TYPE_COLUMN_BYTES)));
	    UniqueConstraint unique = UniqueConstraint
		    .valueOf(Bytes.toString(familyMap.get(HBaseSchema.UNIQUENESS_COLUMN_BYTES)));
	    PropertyDefinition<T> definition = new PropertyDefinition<T>(elementType, propertyKey, type, unique);
	    return definition;
	} catch (IOException | ClassNotFoundException e) {
	    throw new DuctileDBSchemaManagerException("Could not read property names.", e);
	}
    }

    @Override
    public void removePropertyDefinition(ElementType elementType, String propertyKey) {
	try (Table table = graph.getConnection().getTable(HBaseTable.PROPERTY_DEFINITIONS.getTableName())) {
	    Delete delete = new Delete(Bytes.toBytes(propertyKey));
	    switch (elementType) {
	    case VERTEX:
		delete.addFamily(HBaseColumnFamily.VERTEX_DEFINITION.getNameBytes());
		break;
	    case EDGE:
		delete.addFamily(HBaseColumnFamily.EDGE_DEFINITION.getNameBytes());
		break;
	    default:
		throw new DuctileDBSchemaManagerException(
			"Could not delete property definition for element '" + elementType + "'.");
	    }
	    table.delete(delete);
	    graph.getSchema().removeProperty(elementType, propertyKey);
	} catch (IOException e) {
	    throw new DuctileDBSchemaManagerException("Could not read property names.", e);
	}
    }

    @Override
    public Iterable<String> getDefinedTypes() {
	try (Table table = graph.getConnection().getTable(HBaseTable.TYPE_DEFINITIONS.getTableName())) {
	    ResultScanner scanner = table.getScanner(new Scan());
	    Set<String> typeNames = new HashSet<>();
	    scanner.forEach((result) -> typeNames.add(Bytes.toString(result.getRow())));
	    return typeNames;
	} catch (IOException e) {
	    throw new DuctileDBSchemaManagerException("Could not read type names.", e);
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
	try {
	    Connection connection = graph.getConnection();
	    try (Table table = connection.getTable(HBaseTable.TYPE_DEFINITIONS.getTableName())) {
		Put put = new Put(Bytes.toBytes(typeName));
		switch (elementType) {
		case VERTEX:
		    for (String propertyKey : propertyKeys) {
			put.addColumn(HBaseColumnFamily.VERTEX_DEFINITION.getNameBytes(), Bytes.toBytes(propertyKey),
				Bytes.toBytes(0));
		    }
		    break;
		case EDGE:
		    for (String propertyKey : propertyKeys) {
			put.addColumn(HBaseColumnFamily.EDGE_DEFINITION.getNameBytes(), Bytes.toBytes(propertyKey),
				Bytes.toBytes(0));
		    }
		    break;
		default:
		    throw new DuctileDBSchemaManagerException("Cannot define type for element '" + elementType + "'.");
		}
		table.put(put);
		graph.getSchema().defineType(elementType, typeName, propertyKeys);
	    }
	} catch (IOException e) {
	    throw new DuctileDBSchemaManagerException("Could not read property names.", e);
	}
    }

    @Override
    public Set<String> getTypeDefinition(ElementType elementType, String typeName) {
	try (Table table = graph.getConnection().getTable(HBaseTable.TYPE_DEFINITIONS.getTableName())) {
	    Get get = new Get(Bytes.toBytes(typeName));
	    byte[] columnFamily = null;
	    switch (elementType) {
	    case VERTEX:
		columnFamily = HBaseColumnFamily.VERTEX_DEFINITION.getNameBytes();
		break;
	    case EDGE:
		columnFamily = HBaseColumnFamily.EDGE_DEFINITION.getNameBytes();
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
	} catch (IOException e) {
	    throw new DuctileDBSchemaManagerException("Could not read property names.", e);
	}
    }

    @Override
    public void removeTypeDefinition(ElementType elementType, String typeName) {
	Connection connection = graph.getConnection();
	try (Table table = connection.getTable(HBaseTable.TYPE_DEFINITIONS.getTableName())) {
	    Delete delete = new Delete(Bytes.toBytes(typeName));
	    switch (elementType) {
	    case VERTEX:
		delete.addFamily(HBaseColumnFamily.VERTEX_DEFINITION.getNameBytes());
		break;
	    case EDGE:
		delete.addFamily(HBaseColumnFamily.EDGE_DEFINITION.getNameBytes());
		break;
	    default:
		throw new DuctileDBSchemaManagerException(
			"Could not delete type definition for element '" + elementType + "'.");
	    }
	    table.delete(delete);
	    graph.getSchema().removeType(elementType, typeName);
	} catch (IOException e) {
	    throw new DuctileDBSchemaManagerException("Could not read property names.", e);
	}
    }

}

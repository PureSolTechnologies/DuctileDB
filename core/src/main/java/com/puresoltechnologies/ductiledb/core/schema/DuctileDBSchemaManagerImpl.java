package com.puresoltechnologies.ductiledb.core.schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.api.ElementType;
import com.puresoltechnologies.ductiledb.api.manager.DuctileDBSchemaManagerException;
import com.puresoltechnologies.ductiledb.api.schema.DuctileDBPropertyAlreadyDefinedException;
import com.puresoltechnologies.ductiledb.api.schema.DuctileDBSchemaManager;
import com.puresoltechnologies.ductiledb.api.schema.PropertyDefinition;
import com.puresoltechnologies.ductiledb.api.schema.UniqueConstraint;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;

public class DuctileDBSchemaManagerImpl implements DuctileDBSchemaManager {

    private final DuctileDBGraphImpl graph;

    public DuctileDBSchemaManagerImpl(DuctileDBGraphImpl graph) {
	super();
	this.graph = graph;
    }

    @Override
    public Iterable<String> getDefinedProperties() {
	try (Table table = graph.getConnection().getTable(HBaseTable.PROPERTIES.getTableName())) {
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
	if (getPropertyDefinition(definition.getElementType(), definition.getPropertyKey()) != null) {
	    throw new DuctileDBPropertyAlreadyDefinedException(definition);
	}
	try (Table table = graph.getConnection().getTable(HBaseTable.PROPERTIES.getTableName())) {
	    Put put = new Put(Bytes.toBytes(definition.getPropertyKey()));
	    switch (definition.getElementType()) {
	    case VERTEX:
		put.addColumn(HBaseColumnFamily.VERTEX_PROPERTY_DEFINITION.getNameBytes(),
			HBaseSchema.PROPERTY_TYPE_COLUMN_BYTES, Bytes.toBytes(definition.getPropertyType().getName()));
		put.addColumn(HBaseColumnFamily.VERTEX_PROPERTY_DEFINITION.getNameBytes(),
			HBaseSchema.ELEMENT_TYPE_COLUMN_BYTES, Bytes.toBytes(definition.getElementType().name()));
		put.addColumn(HBaseColumnFamily.VERTEX_PROPERTY_DEFINITION.getNameBytes(),
			HBaseSchema.UNIQUENESS_COLUMN_BYTES, Bytes.toBytes(definition.getUniqueConstraint().name()));
		break;
	    case EDGE:
		put.addColumn(HBaseColumnFamily.EDGE_PROPERTY_DEFINITION.getNameBytes(),
			HBaseSchema.PROPERTY_TYPE_COLUMN_BYTES, Bytes.toBytes(definition.getPropertyType().getName()));
		put.addColumn(HBaseColumnFamily.EDGE_PROPERTY_DEFINITION.getNameBytes(),
			HBaseSchema.ELEMENT_TYPE_COLUMN_BYTES, Bytes.toBytes(definition.getElementType().name()));
		put.addColumn(HBaseColumnFamily.EDGE_PROPERTY_DEFINITION.getNameBytes(),
			HBaseSchema.UNIQUENESS_COLUMN_BYTES, Bytes.toBytes(definition.getUniqueConstraint().name()));
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
	try (Table table = graph.getConnection().getTable(HBaseTable.PROPERTIES.getTableName())) {
	    Get get = new Get(Bytes.toBytes(propertyKey));
	    switch (elementType) {
	    case VERTEX:
		get.addFamily(HBaseColumnFamily.VERTEX_PROPERTY_DEFINITION.getNameBytes());
		break;
	    case EDGE:
		get.addFamily(HBaseColumnFamily.EDGE_PROPERTY_DEFINITION.getNameBytes());
		break;
	    default:
		throw new DuctileDBSchemaManagerException("Cannot read property for element '" + elementType + "'.");
	    }
	    Result result = table.get(get);
	    if (result == null) {
		return null;
	    }
	    NavigableMap<byte[], byte[]> familyMap = result
		    .getFamilyMap(HBaseColumnFamily.VERTEX_PROPERTY_DEFINITION.getNameBytes());
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
	try (Table table = graph.getConnection().getTable(HBaseTable.PROPERTIES.getTableName())) {
	    Delete delete = new Delete(Bytes.toBytes(propertyKey));
	    switch (elementType) {
	    case VERTEX:
		delete.addFamily(HBaseColumnFamily.VERTEX_PROPERTY_DEFINITION.getNameBytes());
		break;
	    case EDGE:
		delete.addFamily(HBaseColumnFamily.EDGE_PROPERTY_DEFINITION.getNameBytes());
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

}

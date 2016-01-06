package com.puresoltechnologies.ductiledb.core.manager;

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

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.DuctileDBPropertyAlreadyDefinedException;
import com.puresoltechnologies.ductiledb.api.ElementType;
import com.puresoltechnologies.ductiledb.api.manager.DuctileDBGraphManager;
import com.puresoltechnologies.ductiledb.api.manager.DuctileDBGraphManagerException;
import com.puresoltechnologies.ductiledb.api.manager.PropertyDefinition;
import com.puresoltechnologies.ductiledb.api.manager.UniqueConstraint;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.schema.HBaseColumn;
import com.puresoltechnologies.ductiledb.core.schema.HBaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.schema.HBaseSchema;
import com.puresoltechnologies.ductiledb.core.schema.HBaseTable;
import com.puresoltechnologies.ductiledb.core.utils.Serializer;
import com.puresoltechnologies.versioning.Version;

public class DuctileDBGraphManagerImpl implements DuctileDBGraphManager {

    private final DuctileDBGraphImpl graph;

    public DuctileDBGraphManagerImpl(DuctileDBGraphImpl graph) {
	super();
	this.graph = graph;
    }

    @Override
    public DuctileDBGraph getGraph() {
	return graph;
    }

    @Override
    public Version getVersion() {
	Connection connection = graph.getConnection();
	try (Table table = connection.getTable(HBaseTable.METADATA.getTableName())) {
	    Result result = table.get(new Get(HBaseColumn.SCHEMA_VERSION.getNameBytes()));
	    byte[] version = result.getFamilyMap(HBaseColumnFamily.METADATA.getNameBytes())
		    .get(HBaseColumn.SCHEMA_VERSION.getNameBytes());
	    return Version.valueOf(Bytes.toString(version));
	} catch (IOException e) {
	    throw new DuctileDBGraphManagerException("Could not read variable names.", e);
	}
    }

    @Override
    public Iterable<String> getVariableNames() {
	Connection connection = graph.getConnection();
	try (Table table = connection.getTable(HBaseTable.METADATA.getTableName())) {
	    Set<String> variableNames = new HashSet<>();
	    Result result = table.get(new Get(HBaseColumnFamily.VARIABLES.getNameBytes()));
	    NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(HBaseColumnFamily.VARIABLES.getNameBytes());
	    if (familyMap == null) {
		return variableNames;
	    }
	    for (byte[] nameBytes : familyMap.keySet()) {
		variableNames.add(Bytes.toString(nameBytes));
	    }
	    return variableNames;
	} catch (IOException e) {
	    throw new DuctileDBGraphManagerException("Could not read variable names.", e);
	}
    }

    @Override
    public <T extends Serializable> void setVariable(String variableName, T value) {
	Connection connection = graph.getConnection();
	try (Table table = connection.getTable(HBaseTable.METADATA.getTableName())) {
	    Put put = new Put(HBaseColumnFamily.VARIABLES.getNameBytes());
	    put.addColumn(HBaseColumnFamily.VARIABLES.getNameBytes(), Bytes.toBytes(variableName),
		    Serializer.serializePropertyValue(value));
	    table.put(put);
	} catch (IOException e) {
	    throw new DuctileDBGraphManagerException(
		    "Could not set value '" + value + "' for variable '" + variableName + "'.", e);
	}
    }

    @Override
    public <T> T getVariable(String variableName) {
	Connection connection = graph.getConnection();
	try (Table table = connection.getTable(HBaseTable.METADATA.getTableName())) {
	    Result result = table.get(new Get(HBaseColumnFamily.VARIABLES.getNameBytes()));
	    NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(HBaseColumnFamily.VARIABLES.getNameBytes());
	    if (familyMap == null) {
		return null;
	    }
	    byte[] value = familyMap.get(Bytes.toBytes(variableName));
	    if (value == null) {
		return null;
	    }
	    return Serializer.deserializePropertyValue(value);
	} catch (IOException e) {
	    throw new DuctileDBGraphManagerException("Could not read value for variable '" + variableName + "'.", e);
	}
    }

    @Override
    public void removeVariable(String variableName) {
	Connection connection = graph.getConnection();
	try (Table table = connection.getTable(HBaseTable.METADATA.getTableName())) {
	    Delete delete = new Delete(HBaseColumnFamily.VARIABLES.getNameBytes());
	    delete.addColumn(HBaseColumnFamily.VARIABLES.getNameBytes(), Bytes.toBytes(variableName));
	    table.delete(delete);
	} catch (IOException e) {
	    throw new DuctileDBGraphManagerException("Could not remove variable '" + variableName + "'.", e);
	}
    }

    @Override
    public Iterable<String> getDefinedProperties() {
	try (Table table = graph.getConnection().getTable(HBaseTable.PROPERTIES.getTableName())) {
	    ResultScanner scanner = table.getScanner(new Scan());
	    Set<String> propertyNames = new HashSet<>();
	    scanner.forEach((result) -> propertyNames.add(Bytes.toString(result.getRow())));
	    return propertyNames;
	} catch (IOException e) {
	    throw new DuctileDBGraphManagerException("Could not read property names.", e);
	}
    }

    @Override
    public <T extends Serializable> void defineProperty(PropertyDefinition<T> definition) {
	if (getPropertyDefinition(definition.getPropertyKey()) != null) {
	    throw new DuctileDBPropertyAlreadyDefinedException(definition);
	}
	try (Table table = graph.getConnection().getTable(HBaseTable.PROPERTIES.getTableName())) {
	    Put put = new Put(Bytes.toBytes(definition.getPropertyKey()));
	    put.addColumn(HBaseColumnFamily.DEFINITION.getNameBytes(), HBaseSchema.PROPERTY_TYPE_COLUMN_BYTES,
		    Bytes.toBytes(definition.getPropertyType().getName()));
	    put.addColumn(HBaseColumnFamily.DEFINITION.getNameBytes(), HBaseSchema.ELEMENT_TYPE_COLUMN_BYTES,
		    Bytes.toBytes(definition.getElementType().name()));
	    put.addColumn(HBaseColumnFamily.DEFINITION.getNameBytes(), HBaseSchema.UNIQUENESS_COLUMN_BYTES,
		    Bytes.toBytes(definition.getUniqueConstraint().name()));
	    table.put(put);
	    graph.getSchema().defineProperty(definition);
	} catch (IOException e) {
	    throw new DuctileDBGraphManagerException("Could not read property names.", e);
	}
    }

    @Override
    public <T extends Serializable> PropertyDefinition<T> getPropertyDefinition(String propertyKey) {
	try (Table table = graph.getConnection().getTable(HBaseTable.PROPERTIES.getTableName())) {
	    Get get = new Get(Bytes.toBytes(propertyKey));
	    get.addFamily(HBaseColumnFamily.DEFINITION.getNameBytes());
	    Result result = table.get(get);
	    if (result == null) {
		return null;
	    }
	    NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(HBaseColumnFamily.DEFINITION.getNameBytes());
	    if (familyMap == null) {
		return null;
	    }
	    @SuppressWarnings("unchecked")
	    Class<T> type = (Class<T>) Class
		    .forName(Bytes.toString(familyMap.get(HBaseSchema.PROPERTY_TYPE_COLUMN_BYTES)));
	    ElementType elementType = ElementType
		    .valueOf(Bytes.toString(familyMap.get(HBaseSchema.ELEMENT_TYPE_COLUMN_BYTES)));
	    UniqueConstraint unique = UniqueConstraint
		    .valueOf(Bytes.toString(familyMap.get(HBaseSchema.UNIQUENESS_COLUMN_BYTES)));
	    PropertyDefinition<T> definition = new PropertyDefinition<T>(elementType, propertyKey, type, unique);
	    return definition;
	} catch (IOException | ClassNotFoundException e) {
	    throw new DuctileDBGraphManagerException("Could not read property names.", e);
	}
    }

    @Override
    public void removePropertyDefinition(String propertyKey) {
	try (Table table = graph.getConnection().getTable(HBaseTable.PROPERTIES.getTableName())) {
	    Delete delete = new Delete(Bytes.toBytes(propertyKey));
	    table.delete(delete);
	    graph.getSchema().removeProperty(propertyKey);
	} catch (IOException e) {
	    throw new DuctileDBGraphManagerException("Could not read property names.", e);
	}
    }

}

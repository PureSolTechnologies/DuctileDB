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
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.manager.DuctileDBGraphManager;
import com.puresoltechnologies.ductiledb.api.manager.DuctileDBGraphManagerException;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema;
import com.puresoltechnologies.ductiledb.core.schema.SchemaTable;
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
	try (Table table = connection.getTable(SchemaTable.METADATA.getTableName())) {
	    Result result = table.get(new Get(DuctileDBSchema.SCHEMA_VERSION_COLUMN_BYTES));
	    byte[] version = result.getFamilyMap(DuctileDBSchema.METADATA_COLUMN_FAMILIY_BYTES)
		    .get(DuctileDBSchema.SCHEMA_VERSION_COLUMN_BYTES);
	    return Version.valueOf(Bytes.toString(version));
	} catch (IOException e) {
	    throw new DuctileDBGraphManagerException("Could not read variable names.", e);
	}
    }

    @Override
    public Iterable<String> getVariableNames() {
	Connection connection = graph.getConnection();
	try (Table table = connection.getTable(SchemaTable.METADATA.getTableName())) {
	    Set<String> variableNames = new HashSet<>();
	    Result result = table.get(new Get(DuctileDBSchema.VARIABLES_COLUMN_FAMILIY_BYTES));
	    NavigableMap<byte[], byte[]> familyMap = result
		    .getFamilyMap(DuctileDBSchema.VARIABLES_COLUMN_FAMILIY_BYTES);
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
	try (Table table = connection.getTable(SchemaTable.METADATA.getTableName())) {
	    Put put = new Put(DuctileDBSchema.VARIABLES_COLUMN_FAMILIY_BYTES);
	    put.addColumn(DuctileDBSchema.VARIABLES_COLUMN_FAMILIY_BYTES, Bytes.toBytes(variableName),
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
	try (Table table = connection.getTable(SchemaTable.METADATA.getTableName())) {
	    Result result = table.get(new Get(DuctileDBSchema.VARIABLES_COLUMN_FAMILIY_BYTES));
	    NavigableMap<byte[], byte[]> familyMap = result
		    .getFamilyMap(DuctileDBSchema.VARIABLES_COLUMN_FAMILIY_BYTES);
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
	try (Table table = connection.getTable(SchemaTable.METADATA.getTableName())) {
	    Delete delete = new Delete(DuctileDBSchema.VARIABLES_COLUMN_FAMILIY_BYTES);
	    delete.addColumn(DuctileDBSchema.VARIABLES_COLUMN_FAMILIY_BYTES, Bytes.toBytes(variableName));
	    table.delete(delete);
	} catch (IOException e) {
	    throw new DuctileDBGraphManagerException("Could not remove variable '" + variableName + "'.", e);
	}
    }

}

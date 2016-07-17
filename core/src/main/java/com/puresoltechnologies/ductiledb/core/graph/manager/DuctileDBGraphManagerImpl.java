package com.puresoltechnologies.ductiledb.core.graph.manager;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;

import com.puresoltechnologies.ductiledb.api.graph.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.graph.manager.DuctileDBGraphManager;
import com.puresoltechnologies.ductiledb.api.graph.manager.DuctileDBGraphManagerException;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.graph.schema.HBaseColumn;
import com.puresoltechnologies.ductiledb.core.graph.schema.HBaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.schema.HBaseTable;
import com.puresoltechnologies.ductiledb.core.graph.utils.Serializer;
import com.puresoltechnologies.ductiledb.storage.engine.Delete;
import com.puresoltechnologies.ductiledb.storage.engine.Get;
import com.puresoltechnologies.ductiledb.storage.engine.Put;
import com.puresoltechnologies.ductiledb.storage.engine.Result;
import com.puresoltechnologies.ductiledb.storage.engine.StorageEngine;
import com.puresoltechnologies.ductiledb.storage.engine.Table;
import com.puresoltechnologies.ductiledb.storage.engine.utils.Bytes;
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
	StorageEngine storageEngine = graph.getStorageEngine();
	try (Table table = storageEngine.getTable(HBaseTable.METADATA.getName())) {
	    Result result = table.get(new Get(HBaseColumn.SCHEMA_VERSION.getNameBytes()));
	    byte[] version = result.getFamilyMap(HBaseColumnFamily.METADATA.getName())
		    .get(HBaseColumn.SCHEMA_VERSION.getNameBytes());
	    return Version.valueOf(Bytes.toString(version));
	}
    }

    @Override
    public Iterable<String> getVariableNames() {
	StorageEngine storageEngine = graph.getStorageEngine();
	try (Table table = storageEngine.getTable(HBaseTable.METADATA.getName())) {
	    Set<String> variableNames = new HashSet<>();
	    Result result = table.get(new Get(HBaseColumnFamily.VARIABLES.getNameBytes()));
	    NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(HBaseColumnFamily.VARIABLES.getName());
	    if (familyMap == null) {
		return variableNames;
	    }
	    for (byte[] nameBytes : familyMap.keySet()) {
		variableNames.add(Bytes.toString(nameBytes));
	    }
	    return variableNames;
	}
    }

    @Override
    public <T extends Serializable> void setVariable(String variableName, T value) {
	StorageEngine storageEngine = graph.getStorageEngine();
	try (Table table = storageEngine.getTable(HBaseTable.METADATA.getName())) {
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
	StorageEngine storageEngine = graph.getStorageEngine();
	try (Table table = storageEngine.getTable(HBaseTable.METADATA.getName())) {
	    Result result = table.get(new Get(HBaseColumnFamily.VARIABLES.getNameBytes()));
	    NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(HBaseColumnFamily.VARIABLES.getName());
	    if (familyMap == null) {
		return null;
	    }
	    byte[] value = familyMap.get(Bytes.toBytes(variableName));
	    if (value == null) {
		return null;
	    }
	    return Serializer.deserializePropertyValue(value);
	}
    }

    @Override
    public void removeVariable(String variableName) {
	StorageEngine storageEngine = graph.getStorageEngine();
	try (Table table = storageEngine.getTable(HBaseTable.METADATA.getName())) {
	    Delete delete = new Delete(HBaseColumnFamily.VARIABLES.getNameBytes());
	    delete.addColumns(HBaseColumnFamily.VARIABLES.getNameBytes(), Bytes.toBytes(variableName));
	    table.delete(delete);
	}
    }

}

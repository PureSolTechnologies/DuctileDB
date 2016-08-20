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
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumn;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseTable;
import com.puresoltechnologies.ductiledb.core.graph.utils.Serializer;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.storage.engine.Delete;
import com.puresoltechnologies.ductiledb.storage.engine.Get;
import com.puresoltechnologies.ductiledb.storage.engine.Put;
import com.puresoltechnologies.ductiledb.storage.engine.Result;
import com.puresoltechnologies.ductiledb.storage.engine.Table;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.versioning.Version;

public class DuctileDBGraphManagerImpl implements DuctileDBGraphManager {

    private final DuctileDBGraphImpl graph;
    private final String namespace;

    public DuctileDBGraphManagerImpl(DuctileDBGraphImpl graph) {
	super();
	this.graph = graph;
	this.namespace = graph.getConfiguration().getNamespace();
    }

    @Override
    public DuctileDBGraph getGraph() {
	return graph;
    }

    @Override
    public Version getVersion() {
	DatabaseEngine storageEngine = graph.getStorageEngine();
	Table table = storageEngine.getTable(namespace, DatabaseTable.METADATA.getName());
	Result result;
	try {
	    result = table.get(new Get(DatabaseColumn.SCHEMA_VERSION.getNameBytes()));
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not read version.", e);
	}
	byte[] version = result.getFamilyMap(DatabaseColumnFamily.METADATA.getNameBytes())
		.get(DatabaseColumn.SCHEMA_VERSION.getNameBytes());
	return Version.valueOf(Bytes.toString(version));
    }

    @Override
    public Iterable<String> getVariableNames() {
	DatabaseEngine storageEngine = graph.getStorageEngine();
	Table table = storageEngine.getTable(namespace, DatabaseTable.METADATA.getName());
	Set<String> variableNames = new HashSet<>();
	Result result;
	try {
	    result = table.get(new Get(DatabaseColumnFamily.VARIABLES.getNameBytes()));
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not read variable names.", e);
	}
	NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(DatabaseColumnFamily.VARIABLES.getNameBytes());
	if (familyMap == null) {
	    return variableNames;
	}
	for (byte[] nameBytes : familyMap.keySet()) {
	    variableNames.add(Bytes.toString(nameBytes));
	}
	return variableNames;
    }

    @Override
    public <T extends Serializable> void setVariable(String variableName, T value) {
	try {
	    DatabaseEngine storageEngine = graph.getStorageEngine();
	    Table table = storageEngine.getTable(namespace, DatabaseTable.METADATA.getName());
	    Put put = new Put(DatabaseColumnFamily.VARIABLES.getNameBytes());
	    put.addColumn(DatabaseColumnFamily.VARIABLES.getNameBytes(), Bytes.toBytes(variableName),
		    Serializer.serializePropertyValue(value));
	    table.put(put);
	} catch (IOException | StorageException e) {
	    throw new DuctileDBGraphManagerException(
		    "Could not set value '" + value + "' for variable '" + variableName + "'.", e);
	}
    }

    @Override
    public <T> T getVariable(String variableName) {
	DatabaseEngine storageEngine = graph.getStorageEngine();
	Table table = storageEngine.getTable(namespace, DatabaseTable.METADATA.getName());
	Result result;
	try {
	    result = table.get(new Get(DatabaseColumnFamily.VARIABLES.getNameBytes()));
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not read variable.", e);
	}
	NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(DatabaseColumnFamily.VARIABLES.getNameBytes());
	if (familyMap == null) {
	    return null;
	}
	byte[] value = familyMap.get(Bytes.toBytes(variableName));
	if (value == null) {
	    return null;
	}
	return Serializer.deserializePropertyValue(value);
    }

    @Override
    public void removeVariable(String variableName) {
	DatabaseEngine storageEngine = graph.getStorageEngine();
	Table table = storageEngine.getTable(namespace, DatabaseTable.METADATA.getName());
	Delete delete = new Delete(DatabaseColumnFamily.VARIABLES.getNameBytes());
	delete.addColumns(DatabaseColumnFamily.VARIABLES.getNameBytes(), Bytes.toBytes(variableName));
	try {
	    table.delete(delete);
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not remove variable.", e);
	}
    }

}

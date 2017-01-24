package com.puresoltechnologies.ductiledb.core.graph.manager;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;

import com.puresoltechnologies.ductiledb.bigtable.BigTable;
import com.puresoltechnologies.ductiledb.bigtable.Delete;
import com.puresoltechnologies.ductiledb.bigtable.Get;
import com.puresoltechnologies.ductiledb.bigtable.Put;
import com.puresoltechnologies.ductiledb.bigtable.Result;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnValue;
import com.puresoltechnologies.ductiledb.core.graph.GraphStore;
import com.puresoltechnologies.ductiledb.core.graph.GraphStoreImpl;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumn;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseTable;
import com.puresoltechnologies.ductiledb.core.graph.utils.Serializer;
import com.puresoltechnologies.ductiledb.engine.Namespace;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.versioning.Version;

public class DuctileDBGraphManagerImpl implements DuctileDBGraphManager {

    private final GraphStoreImpl graph;
    private final Namespace namespace;

    public DuctileDBGraphManagerImpl(GraphStoreImpl graph) {
	super();
	this.graph = graph;
	this.namespace = graph.getStorageEngine().getNamespace(graph.getConfiguration().getNamespace());
    }

    @Override
    public GraphStore getGraph() {
	return graph;
    }

    @Override
    public Version getVersion() {
	BigTable table = namespace.getTable(DatabaseTable.METADATA.getName());
	Result result;
	try {
	    result = table.get(new Get(DatabaseColumn.SCHEMA_VERSION.getKey()));
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not read version.", e);
	}
	ColumnValue version = result.getFamilyMap(DatabaseColumnFamily.METADATA.getKey())
		.get(DatabaseColumn.SCHEMA_VERSION.getKey());
	return Version.valueOf(version.toString());
    }

    @Override
    public Iterable<String> getVariableNames() {
	BigTable table = namespace.getTable(DatabaseTable.METADATA.getName());
	Set<String> variableNames = new HashSet<>();
	Result result;
	try {
	    result = table.get(new Get(DatabaseColumnFamily.VARIABLES.getKey()));
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not read variable names.", e);
	}
	NavigableMap<Key, ColumnValue> familyMap = result.getFamilyMap(DatabaseColumnFamily.VARIABLES.getKey());
	if (familyMap == null) {
	    return variableNames;
	}
	for (Key column : familyMap.keySet()) {
	    variableNames.add(column.toStringValue());
	}
	return variableNames;
    }

    @Override
    public <T extends Serializable> void setVariable(String variableName, T value) {
	try {
	    BigTable table = namespace.getTable(DatabaseTable.METADATA.getName());
	    Put put = new Put(DatabaseColumnFamily.VARIABLES.getKey());
	    put.addColumn(DatabaseColumnFamily.VARIABLES.getKey(), Key.of(variableName),
		    ColumnValue.of(Serializer.serializePropertyValue(value)));
	    table.put(put);
	} catch (IOException | StorageException e) {
	    throw new DuctileDBGraphManagerException(
		    "Could not set value '" + value + "' for variable '" + variableName + "'.", e);
	}
    }

    @Override
    public <T> T getVariable(String variableName) {
	BigTable table = namespace.getTable(DatabaseTable.METADATA.getName());
	Result result;
	try {
	    result = table.get(new Get(DatabaseColumnFamily.VARIABLES.getKey()));
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not read variable.", e);
	}
	NavigableMap<Key, ColumnValue> familyMap = result.getFamilyMap(DatabaseColumnFamily.VARIABLES.getKey());
	if (familyMap == null) {
	    return null;
	}
	ColumnValue value = familyMap.get(Key.of(variableName));
	if (value == null) {
	    return null;
	}
	return Serializer.deserializePropertyValue(value.getBytes());
    }

    @Override
    public void removeVariable(String variableName) {
	BigTable table = namespace.getTable(DatabaseTable.METADATA.getName());
	Delete delete = new Delete(DatabaseColumnFamily.VARIABLES.getKey());
	delete.addColumns(DatabaseColumnFamily.VARIABLES.getKey(), Key.of(variableName));
	try {
	    table.delete(delete);
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not remove variable.", e);
	}
    }

}

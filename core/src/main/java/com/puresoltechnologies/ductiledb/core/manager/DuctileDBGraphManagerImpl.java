package com.puresoltechnologies.ductiledb.core.manager;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.manager.DuctileDBGraphManager;
import com.puresoltechnologies.ductiledb.api.manager.DuctileDBGraphManagerException;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema;
import com.puresoltechnologies.ductiledb.core.schema.SchemaTable;
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

	    return null;
	} catch (IOException e) {
	    throw new DuctileDBGraphManagerException("Could not read variable names.", e);
	}
    }

    @Override
    public <T> void setVariable(String variableName, T value) {
	// TODO Auto-generated method stub

    }

    @Override
    public <T> T getVariable(String variableName) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public void removeVariable(String variableName) {
	// TODO Auto-generated method stub

    }

}

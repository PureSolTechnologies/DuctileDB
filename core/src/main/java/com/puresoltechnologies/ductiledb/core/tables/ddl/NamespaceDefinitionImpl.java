package com.puresoltechnologies.ductiledb.core.tables.ddl;

/**
 * This class contains the information for the namespace.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class NamespaceDefinitionImpl implements NamespaceDefinition {

    private final String database;
    private final String name;

    public NamespaceDefinitionImpl(String name) {
	this("table_store", name);
    }

    public NamespaceDefinitionImpl(String database, String name) {
	super();
	this.database = database;
	this.name = name;
    }

    @Override
    public String getDatabaseName() {
	return database;
    }

    @Override
    public String getName() {
	return name;
    }

}

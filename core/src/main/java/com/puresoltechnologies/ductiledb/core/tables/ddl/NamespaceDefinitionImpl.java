package com.puresoltechnologies.ductiledb.core.tables.ddl;

/**
 * This class contains the information for the namespace.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class NamespaceDefinitionImpl implements NamespaceDefinition {

    private final String name;

    public NamespaceDefinitionImpl(String name) {
	super();
	this.name = name;
    }

    @Override
    public String getName() {
	return name;
    }

}

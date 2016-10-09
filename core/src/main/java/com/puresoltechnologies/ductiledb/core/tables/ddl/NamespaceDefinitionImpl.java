package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.api.tables.ddl.NamespaceDefinition;

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

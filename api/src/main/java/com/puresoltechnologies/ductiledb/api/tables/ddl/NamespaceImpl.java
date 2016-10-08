package com.puresoltechnologies.ductiledb.api.tables.ddl;

/**
 * This class contains the information for the namespace.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class NamespaceImpl implements Namespace {

    private final String name;

    public NamespaceImpl(String name) {
	super();
	this.name = name;
    }

    @Override
    public String getName() {
	return name;
    }

}

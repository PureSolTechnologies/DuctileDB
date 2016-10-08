package com.puresoltechnologies.ductiledb.api.tables.ddl;

public class TableImpl implements Table {

    private final String name;

    public TableImpl(String name) {
	super();
	this.name = name;
    }

    @Override
    public String getName() {
	return name;
    }

}

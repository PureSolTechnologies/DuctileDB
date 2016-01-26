package com.puresoltechnologies.ductiledb.xo.impl.metadata;

import java.io.Serializable;

import org.apache.tinkerpop.gremlin.structure.Element;

public class DuctileIndexedPropertyMetadata {

    private final String name;
    private final boolean unique;
    private final Class<? extends Element> type;
    private final Class<? extends Serializable> dataType;

    public DuctileIndexedPropertyMetadata(String name, boolean unique, Class<? extends Serializable> dataType,
	    Class<? extends Element> type) {
	this.name = name;
	this.unique = unique;
	this.dataType = dataType;
	this.type = type;
    }

    public String getName() {
	return name;
    }

    public boolean isUnique() {
	return unique;
    }

    public Class<? extends Element> getType() {
	return type;
    }

    public Class<? extends Serializable> getDataType() {
	return dataType;
    }

}

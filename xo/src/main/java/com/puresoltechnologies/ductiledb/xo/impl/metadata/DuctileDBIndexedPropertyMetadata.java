package com.puresoltechnologies.ductiledb.xo.impl.metadata;

import com.tinkerpop.blueprints.Element;

public class DuctileDBIndexedPropertyMetadata {

	private final String name;
	private final boolean unique;
	private final Class<? extends Element> type;
	private final Class<?> dataType;

	public DuctileDBIndexedPropertyMetadata(String name, boolean unique,
			Class<?> dataType, Class<? extends Element> type) {
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

	public Class<?> getDataType() {
		return dataType;
	}

}

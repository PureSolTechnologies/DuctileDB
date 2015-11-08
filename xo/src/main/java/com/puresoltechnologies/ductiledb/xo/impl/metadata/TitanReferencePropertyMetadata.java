package com.puresoltechnologies.ductiledb.xo.impl.metadata;

import com.tinkerpop.blueprints.Direction;

public class TitanReferencePropertyMetadata {

	private final String name;
	private final Direction direction;

	public TitanReferencePropertyMetadata(String name, Direction direction) {
		this.name = name;
		this.direction = direction;
	}

	public String getName() {
		return name;
	}

	public Direction getDirection() {
		return direction;
	}

}

package com.puresoltechnologies.ductiledb.xo.impl.metadata;

import com.puresoltechnologies.ductiledb.api.EdgeDirection;

public class DuctileDBReferencePropertyMetadata {

    private final String name;
    private final EdgeDirection direction;

    public DuctileDBReferencePropertyMetadata(String name, EdgeDirection direction) {
	this.name = name;
	this.direction = direction;
    }

    public String getName() {
	return name;
    }

    public EdgeDirection getDirection() {
	return direction;
    }

}

package com.puresoltechnologies.ductiledb.xo.impl.metadata;

import com.puresoltechnologies.ductiledb.api.EdgeDirection;

public class DuctileDBCollectionPropertyMetadata {

    private final String name;
    private final EdgeDirection direction;

    public DuctileDBCollectionPropertyMetadata(String name, EdgeDirection direction) {
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

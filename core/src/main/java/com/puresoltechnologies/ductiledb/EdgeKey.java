package com.puresoltechnologies.ductiledb;

import java.io.Serializable;

import org.apache.commons.lang.SerializationUtils;

import com.tinkerpop.blueprints.Direction;

public class EdgeKey implements Serializable {

    private static final long serialVersionUID = -5352519401861462834L;

    public static EdgeKey decode(byte[] edgeKey) {
	return (EdgeKey) SerializationUtils.deserialize(edgeKey);
    }

    private final Direction direction;
    private final Object id;
    private final String edgeType;

    public EdgeKey(Direction direction, Object id, String edgeType) {
	super();
	this.direction = direction;
	this.id = id;
	this.edgeType = edgeType;
    }

    public Direction getDirection() {
	return direction;
    }

    public Object getId() {
	return id;
    }

    public String getEdgeType() {
	return edgeType;
    }

    public byte[] encode() {
	return SerializationUtils.serialize(this);
    }

}

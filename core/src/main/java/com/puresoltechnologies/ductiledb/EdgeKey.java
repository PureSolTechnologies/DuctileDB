package com.puresoltechnologies.ductiledb;

import java.io.Serializable;
import java.util.Arrays;

import com.puresoltechnologies.ductiledb.utils.IdEncoder;
import com.tinkerpop.blueprints.Direction;

public class EdgeKey implements Serializable {

    private static final long serialVersionUID = -5352519401861462834L;

    public static EdgeKey decode(byte[] edgeKey) {
	Direction direction;
	if (edgeKey[0] == 0) {
	    direction = Direction.IN;
	} else if (edgeKey[0] == 1) {
	    direction = Direction.OUT;
	} else {
	    throw new IllegalArgumentException("Direction byte '" + edgeKey[0] + "' is not supported.");
	}
	long id = IdEncoder.decodeRowId(edgeKey, 1);
	long vertexId = IdEncoder.decodeRowId(edgeKey, 9);
	String edgeType = new String(Arrays.copyOfRange(edgeKey, 17, edgeKey.length));
	return new EdgeKey(direction, id, vertexId, edgeType);
    }

    private final Direction direction;
    private final long id;
    private final long vertexId;
    private final String edgeType;

    public EdgeKey(Direction direction, long id, long vertexId, String edgeType) {
	super();
	if ((direction != Direction.IN) && (direction != Direction.OUT)) {
	    throw new IllegalArgumentException("Direction needs to be IN or OUT.");
	}
	this.direction = direction;
	this.id = id;
	this.vertexId = vertexId;
	this.edgeType = edgeType;
    }

    public Direction getDirection() {
	return direction;
    }

    public long getId() {
	return id;
    }

    public long getVertexId() {
	return vertexId;
    }

    public String getEdgeType() {
	return edgeType;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((direction == null) ? 0 : direction.hashCode());
	result = prime * result + ((edgeType == null) ? 0 : edgeType.hashCode());
	result = prime * result + (int) (id ^ (id >>> 32));
	result = prime * result + (int) (vertexId ^ (vertexId >>> 32));
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	EdgeKey other = (EdgeKey) obj;
	if (direction != other.direction)
	    return false;
	if (edgeType == null) {
	    if (other.edgeType != null)
		return false;
	} else if (!edgeType.equals(other.edgeType))
	    return false;
	if (id != other.id)
	    return false;
	if (vertexId != other.vertexId)
	    return false;
	return true;
    }

    public byte[] encode() {
	byte[] type = edgeType.getBytes();
	byte[] encoded = new byte[17 + type.length];
	switch (direction) {
	case IN:
	    encoded[0] = 0;
	    break;
	case OUT:
	    encoded[0] = 1;
	    break;
	default:
	    throw new RuntimeException("Direction '" + direction + "' is not supported.");
	}
	long tempId = id;
	for (int i = 0; i < 8; ++i) {
	    encoded[1 + i] = (byte) (tempId & 0xff);
	    tempId >>= 8;
	}
	tempId = vertexId;
	for (int i = 0; i < 8; ++i) {
	    encoded[9 + i] = (byte) (tempId & 0xff);
	    tempId >>= 8;
	}
	for (int i = 0; i < type.length; ++i) {
	    encoded[17 + i] = type[i];
	}
	return encoded;
    }

}

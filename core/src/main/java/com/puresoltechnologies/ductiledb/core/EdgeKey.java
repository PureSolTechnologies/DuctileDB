package com.puresoltechnologies.ductiledb.core;

import java.io.Serializable;
import java.util.Arrays;

import com.puresoltechnologies.ductiledb.api.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.core.utils.StringEncoder;

public class EdgeKey implements Serializable {

    private static final long serialVersionUID = -5352519401861462834L;

    public static EdgeKey decode(byte[] edgeKey) {
	EdgeDirection direction;
	if (edgeKey[0] == 0) {
	    direction = EdgeDirection.IN;
	} else if (edgeKey[0] == 1) {
	    direction = EdgeDirection.OUT;
	} else {
	    throw new IllegalArgumentException("Direction byte '" + edgeKey[0] + "' is not supported.");
	}
	long id = IdEncoder.decodeRowId(edgeKey, 1);
	long vertexId = IdEncoder.decodeRowId(edgeKey, 9);
	String type = StringEncoder.decode(Arrays.copyOfRange(edgeKey, 17, edgeKey.length));
	return new EdgeKey(direction, id, vertexId, type);
    }

    private final EdgeDirection direction;
    private final long id;
    private final long vertexId;
    private final String type;

    /**
     * Constructor needed for serialization.
     */
    public EdgeKey() {
	super();
	direction = null;
	id = -1;
	vertexId = -1;
	type = null;
    }

    public EdgeKey(EdgeDirection direction, long id, long vertexId, String type) {
	super();
	if ((direction != EdgeDirection.IN) && (direction != EdgeDirection.OUT)) {
	    throw new IllegalArgumentException("Direction needs to be IN or OUT.");
	}
	this.direction = direction;
	this.id = id;
	this.vertexId = vertexId;
	this.type = type;
    }

    public EdgeDirection getDirection() {
	return direction;
    }

    public long getId() {
	return id;
    }

    public long getVertexId() {
	return vertexId;
    }

    public String getType() {
	return type;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((direction == null) ? 0 : direction.hashCode());
	result = prime * result + ((type == null) ? 0 : type.hashCode());
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
	if (type == null) {
	    if (other.type != null)
		return false;
	} else if (!type.equals(other.type))
	    return false;
	if (id != other.id)
	    return false;
	if (vertexId != other.vertexId)
	    return false;
	return true;
    }

    public byte[] encode() {
	byte[] typeBytes = StringEncoder.encode(type);
	byte[] encoded = new byte[17 + typeBytes.length];
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
	for (int i = 0; i < typeBytes.length; ++i) {
	    encoded[17 + i] = typeBytes[i];
	}
	return encoded;
    }

    @Override
    public String toString() {
	StringBuffer buffer = new StringBuffer();
	buffer.append(direction.name());
	buffer.append(":");
	buffer.append(type);
	buffer.append("=");
	buffer.append(id);
	buffer.append("; vertex=");
	buffer.append(vertexId);
	return buffer.toString();
    }
}

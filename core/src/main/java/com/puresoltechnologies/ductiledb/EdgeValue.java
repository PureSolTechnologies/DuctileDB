package com.puresoltechnologies.ductiledb;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.SerializationUtils;

public class EdgeValue implements Serializable {

    private static final long serialVersionUID = -1991386644968007525L;

    public static EdgeValue decode(byte[] edgeKey) {
	return (EdgeValue) SerializationUtils.deserialize(edgeKey);
    }

    private final Map<String, Object> properties = new HashMap<>();

    public EdgeValue(Map<String, Object> properties) {
	this.properties.putAll(properties);
    }

    public Map<String, Object> getProperties() {
	return properties;
    }

    public byte[] encode() {
	return SerializationUtils.serialize(this);
    }

}

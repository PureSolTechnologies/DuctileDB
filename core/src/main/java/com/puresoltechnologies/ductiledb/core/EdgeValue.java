package com.puresoltechnologies.ductiledb.core;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;
import com.puresoltechnologies.ductiledb.core.utils.Serializer;

public class EdgeValue implements Serializable {

    private static final long serialVersionUID = -1991386644968007525L;

    public static EdgeValue decode(byte[] bytes) {
	EdgeValue value = Serializer.deserialize(bytes, EdgeValue.class);
	if (value.properties == null) {
	    ElementUtils.setFinalField(value, EdgeValue.class, "properties", new HashMap<>());
	}
	return value;
    }

    private final Map<String, Object> properties = new HashMap<>();

    /**
     * Constructor needed for serialization.
     */
    public EdgeValue() {
    }

    public EdgeValue(Map<String, Object> properties) {
	this.properties.putAll(properties);
    }

    public Map<String, Object> getProperties() {
	return properties;
    }

    public byte[] encode() throws IOException {
	return Serializer.serialize(this, EdgeValue.class);
    }

}

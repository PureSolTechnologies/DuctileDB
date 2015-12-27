package com.puresoltechnologies.ductiledb.core;

import com.puresoltechnologies.ductiledb.api.DuctileDBElement;

public abstract class AbstractDuctileDBElement implements DuctileDBElement {

    protected final String getPropertiesString() {
	StringBuilder builder = new StringBuilder("{");
	boolean first = true;
	for (String key : getPropertyKeys()) {
	    if (first) {
		first = false;
	    } else {
		builder.append(", ");
	    }
	    Object value = getProperty(key);
	    builder.append(key);
	    builder.append('=');
	    builder.append(value);
	}
	builder.append('}');
	return builder.toString();
    }

    @Override
    public DuctileDBElement clone() {
	try {
	    return (DuctileDBElement) super.clone();
	} catch (CloneNotSupportedException e) {
	    throw new RuntimeException(e);
	}
    }

}

package com.puresoltechnologies.ductiledb.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.puresoltechnologies.ductiledb.api.DuctileDBElement;
import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;

public abstract class DuctileDBDetachedElementImpl implements DuctileDBElement {

    private final Map<String, Object> properties;
    private final DuctileDBGraphImpl graph;
    private final long id;

    public DuctileDBDetachedElementImpl(DuctileDBGraphImpl graph, long id) {
	this(graph, id, new HashMap<>());
    }

    public DuctileDBDetachedElementImpl(DuctileDBGraphImpl graph, long id, Map<String, Object> properties) {
	super();
	this.graph = graph;
	this.id = id;
	this.properties = Collections.unmodifiableMap(properties);
    }

    private void throwDetachedException() {
	throw new DuctileDBException("This element is detached from graph and cannot be altered.");
    }

    @Override
    public DuctileDBGraphImpl getGraph() {
	return graph;
    }

    @Override
    public final long getId() {
	return id;
    }

    @Override
    public final Set<String> getPropertyKeys() {
	return properties.keySet();
    }

    @Override
    public final <T> void setProperty(String key, T value) {
	throwDetachedException();
    }

    public final void setPropertyInternally(String key, Object value) {
	throwDetachedException();
    }

    protected abstract <T> void setProperty(DuctileDBGraph graph, String key, T value);

    @Override
    public final <T> T getProperty(String key) {
	@SuppressWarnings("unchecked")
	T t = (T) properties.get(key);
	return t;
    }

    @Override
    public final void removeProperty(String key) {
	throwDetachedException();
    }

    public final void removePropertyInternally(String key) {
	throwDetachedException();
    }

    protected abstract void removeProperty(DuctileDBGraph graph, String key);

    protected String getPropertiesString() {
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
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + (int) (id ^ (id >>> 32));
	result = prime * result + ((properties == null) ? 0 : properties.hashCode());
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
	DuctileDBDetachedElementImpl other = (DuctileDBDetachedElementImpl) obj;
	if (id != other.id)
	    return false;
	if (properties == null) {
	    if (other.properties != null)
		return false;
	} else if (!properties.equals(other.properties))
	    return false;
	return true;
    }

    @Override
    public DuctileDBDetachedElementImpl clone() {
	try {
	    DuctileDBDetachedElementImpl cloned = (DuctileDBDetachedElementImpl) super.clone();
	    ElementUtils.setFinalField(cloned, DuctileDBDetachedElementImpl.class, "id", id);
	    ElementUtils.setFinalField(cloned, DuctileDBDetachedElementImpl.class, "graph", graph);
	    ElementUtils.setFinalField(cloned, DuctileDBDetachedElementImpl.class, "properties",
		    new HashMap<>(properties));
	    return cloned;
	} catch (CloneNotSupportedException e) {
	    throw new RuntimeException(e);
	}
    }
}

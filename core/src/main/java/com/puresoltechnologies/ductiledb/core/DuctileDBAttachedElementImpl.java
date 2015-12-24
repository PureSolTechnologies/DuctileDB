package com.puresoltechnologies.ductiledb.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.puresoltechnologies.ductiledb.api.DuctileDBElement;
import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;

public abstract class DuctileDBAttachedElementImpl implements DuctileDBElement {

    private final Map<String, Object> properties = new HashMap<>();
    private final DuctileDBGraphImpl graph;
    private final long id;

    public DuctileDBAttachedElementImpl(DuctileDBGraphImpl graph, long id) {
	super();
	this.graph = graph;
	this.id = id;
    }

    public DuctileDBAttachedElementImpl(DuctileDBGraphImpl graph, long id, Map<String, Object> properties) {
	this(graph, id);
	this.properties.putAll(properties);
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
	setPropertyInternally(key, value);
	setProperty(graph, key, value);
    }

    public final void setPropertyInternally(String key, Object value) {
	if (DuctileDBElement.class.isAssignableFrom(value.getClass())) {
	    throw new IllegalArgumentException("A graph element is not allowed to be used as property value.");
	}
	properties.put(key, value);
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
	removeProperty(graph, key);
	removePropertyInternally(key);
    }

    public final void removePropertyInternally(String key) {
	properties.remove(key);
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
	DuctileDBAttachedElementImpl other = (DuctileDBAttachedElementImpl) obj;
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
    public DuctileDBAttachedElementImpl clone() {
	try {
	    DuctileDBAttachedElementImpl cloned = (DuctileDBAttachedElementImpl) super.clone();
	    ElementUtils.setFinalField(cloned, DuctileDBAttachedElementImpl.class, "id", id);
	    ElementUtils.setFinalField(cloned, DuctileDBAttachedElementImpl.class, "graph", graph);
	    ElementUtils.setFinalField(cloned, DuctileDBAttachedElementImpl.class, "properties",
		    new HashMap<>(properties));
	    return cloned;
	} catch (CloneNotSupportedException e) {
	    throw new RuntimeException(e);
	}
    }
}

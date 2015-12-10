package com.puresoltechnologies.ductiledb.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.puresoltechnologies.ductiledb.api.DuctileDBElement;
import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;

public abstract class DuctileDBElementImpl implements DuctileDBElement {

    private final Map<String, Object> properties = new HashMap<>();
    private final DuctileDBGraphImpl graph;
    private final long id;

    public DuctileDBElementImpl(DuctileDBGraphImpl graph, long id) {
	super();
	this.graph = graph;
	this.id = id;
    }

    public DuctileDBElementImpl(DuctileDBGraphImpl graph, long id, Map<String, Object> properties) {
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
    public final void setProperty(String key, Object value) {
	setProperty(graph, key, value);
	properties.put(key, value);
    }

    protected abstract void setProperty(DuctileDBGraph graph, String key, Object value);

    @Override
    public final <T> T getProperty(String key) {
	@SuppressWarnings("unchecked")
	T t = (T) properties.get(key);
	return t;
    }

    @Override
    public final <T> T removeProperty(String key) {
	@SuppressWarnings("unchecked")
	T value = (T) getProperty(key);
	removeProperty(graph, key);
	properties.remove(key);
	return value;
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
	DuctileDBElementImpl other = (DuctileDBElementImpl) obj;
	if (id != other.id)
	    return false;
	if (properties == null) {
	    if (other.properties != null)
		return false;
	} else if (!properties.equals(other.properties))
	    return false;
	return true;
    }

}

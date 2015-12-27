package com.puresoltechnologies.ductiledb.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.exceptions.DuctileDBException;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;

public abstract class DuctileDBDetachedElement extends AbstractDuctileDBElement {

    private final Map<String, Object> properties;
    private final DuctileDBGraphImpl graph;
    private final long id;

    public DuctileDBDetachedElement(DuctileDBGraphImpl graph, long id) {
	this(graph, id, new HashMap<>());
    }

    public DuctileDBDetachedElement(DuctileDBGraphImpl graph, long id, Map<String, Object> properties) {
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

    protected abstract void removeProperty(DuctileDBGraph graph, String key);

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
	DuctileDBDetachedElement other = (DuctileDBDetachedElement) obj;
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
    public DuctileDBDetachedElement clone() {
	DuctileDBDetachedElement cloned = (DuctileDBDetachedElement) super.clone();
	ElementUtils.setFinalField(cloned, DuctileDBDetachedElement.class, "id", id);
	ElementUtils.setFinalField(cloned, DuctileDBDetachedElement.class, "graph", graph);
	ElementUtils.setFinalField(cloned, DuctileDBDetachedElement.class, "properties", new HashMap<>(properties));
	return cloned;
    }
}

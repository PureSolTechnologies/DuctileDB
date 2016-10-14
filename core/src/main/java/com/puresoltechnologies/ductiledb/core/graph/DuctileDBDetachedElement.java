package com.puresoltechnologies.ductiledb.core.graph;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.puresoltechnologies.ductiledb.core.DuctileDBException;
import com.puresoltechnologies.ductiledb.core.graph.tx.DuctileDBTransactionImpl;
import com.puresoltechnologies.ductiledb.core.graph.utils.ElementUtils;

public abstract class DuctileDBDetachedElement extends AbstractDuctileDBElement {

    private final Map<String, Object> properties;

    public DuctileDBDetachedElement(DuctileDBTransactionImpl transaction, long id) {
	this(transaction, id, new HashMap<>());
    }

    public DuctileDBDetachedElement(DuctileDBTransactionImpl transaction, long id, Map<String, Object> properties) {
	super(transaction, id);
	if (properties == null) {
	    throw new IllegalArgumentException("Properties must not be null.");
	}
	this.properties = Collections.unmodifiableMap(properties);
    }

    private void throwDetachedException() {
	throw new DuctileDBException("This element is detached from graph and cannot be altered.");
    }

    @Override
    public final Set<String> getPropertyKeys() {
	return properties.keySet();
    }

    @Override
    public final <T> void setProperty(String key, T value) {
	throwDetachedException();
    }

    protected abstract <T> void setProperty(GraphStore graph, String key, T value);

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

    protected abstract void removeProperty(GraphStore graph, String key);

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = super.hashCode();
	result = prime * result + ((properties == null) ? 0 : properties.hashCode());
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (!super.equals(obj))
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	DuctileDBDetachedElement other = (DuctileDBDetachedElement) obj;
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
	ElementUtils.setFinalField(cloned, DuctileDBDetachedElement.class, "properties", new HashMap<>(properties));
	return cloned;
    }
}

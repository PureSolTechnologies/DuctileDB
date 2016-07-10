package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.puresoltechnologies.ductiledb.api.graph.DuctileDBElement;
import com.puresoltechnologies.ductiledb.core.graph.AbstractDuctileDBElement;
import com.puresoltechnologies.ductiledb.core.graph.utils.ElementUtils;

abstract class DuctileDBCacheElement extends AbstractDuctileDBElement {

    private final Map<String, Object> properties = new HashMap<>();

    public DuctileDBCacheElement(DuctileDBTransactionImpl transaction, long id, Map<String, Object> properties) {
	super(transaction, id);
	if (properties == null) {
	    throw new IllegalArgumentException("Properties must not be null.");
	}
	this.properties.putAll(properties);
    }

    @Override
    public final Set<String> getPropertyKeys() {
	return properties.keySet();
    }

    @Override
    public final <T> void setProperty(String key, T value) {
	if (DuctileDBElement.class.isAssignableFrom(value.getClass())) {
	    throw new IllegalArgumentException("A graph element is not allowed to be used as property value.");
	}
	properties.put(key, value);
    }

    @Override
    public final <T> T getProperty(String key) {
	@SuppressWarnings("unchecked")
	T t = (T) properties.get(key);
	return t;
    }

    @Override
    public final void removeProperty(String key) {
	properties.remove(key);
    }

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
	DuctileDBCacheElement other = (DuctileDBCacheElement) obj;
	if (properties == null) {
	    if (other.properties != null)
		return false;
	} else if (!properties.equals(other.properties))
	    return false;
	return true;
    }

    @Override
    public DuctileDBCacheElement clone() {
	DuctileDBCacheElement cloned = (DuctileDBCacheElement) super.clone();
	ElementUtils.setFinalField(cloned, DuctileDBCacheElement.class, "properties", new HashMap<>(properties));
	return cloned;
    }
}

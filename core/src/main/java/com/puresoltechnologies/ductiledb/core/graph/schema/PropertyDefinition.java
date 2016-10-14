package com.puresoltechnologies.ductiledb.core.graph.schema;

import java.io.Serializable;

import com.puresoltechnologies.ductiledb.core.graph.ElementType;

public class PropertyDefinition<T extends Serializable> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ElementType elementType;
    private final String propertyKey;
    private final Class<T> propertyType;
    private final UniqueConstraint uniqueConstraint;

    /**
     * Default constructor is needed for serialization.
     */
    public PropertyDefinition() {
	super();
	this.elementType = null;
	this.propertyKey = null;
	this.propertyType = null;
	this.uniqueConstraint = null;
    }

    public PropertyDefinition(ElementType elementType, String propertyKey, Class<T> propertyType,
	    UniqueConstraint uniqueConstraint) {
	super();
	if (elementType == null) {
	    throw new IllegalArgumentException("elementType must not be null.");
	}
	this.elementType = elementType;
	if ((propertyKey == null) || (propertyKey.isEmpty())) {
	    throw new IllegalArgumentException("propertyKey must not be null or empty.");
	}
	this.propertyKey = propertyKey;
	if (propertyType == null) {
	    throw new IllegalArgumentException("propertyType must not be null.");
	}
	this.propertyType = propertyType;
	if (uniqueConstraint == null) {
	    throw new IllegalArgumentException("uniqueConstraint must not be null.");
	}
	this.uniqueConstraint = uniqueConstraint;
    }

    public final ElementType getElementType() {
	return elementType;
    }

    public final String getPropertyKey() {
	return propertyKey;
    }

    public final Class<T> getPropertyType() {
	return propertyType;
    }

    public final UniqueConstraint getUniqueConstraint() {
	return uniqueConstraint;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((elementType == null) ? 0 : elementType.hashCode());
	result = prime * result + ((propertyKey == null) ? 0 : propertyKey.hashCode());
	result = prime * result + ((propertyType == null) ? 0 : propertyType.hashCode());
	result = prime * result + ((uniqueConstraint == null) ? 0 : uniqueConstraint.hashCode());
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
	PropertyDefinition<?> other = (PropertyDefinition<?>) obj;
	if (elementType != other.elementType)
	    return false;
	if (propertyKey == null) {
	    if (other.propertyKey != null)
		return false;
	} else if (!propertyKey.equals(other.propertyKey))
	    return false;
	if (propertyType == null) {
	    if (other.propertyType != null)
		return false;
	} else if (!propertyType.equals(other.propertyType))
	    return false;
	if (uniqueConstraint != other.uniqueConstraint)
	    return false;
	return true;
    }

    @Override
    public String toString() {
	return "Property definition for '" + propertyKey + "': type='" + propertyType.getName() + "', element='"
		+ elementType.name() + "', unique='" + uniqueConstraint.name() + "'";
    }
}

package com.puresoltechnologies.ductiledb.api.manager;

import java.io.Serializable;

import com.puresoltechnologies.ductiledb.api.ElementType;

public class PropertyDefinition<T extends Serializable> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ElementType elementType;
    private final String propertyName;
    private final Class<T> propertyType;
    private final UniqueConstraint uniqueConstraint;

    /**
     * Default constructor is needed for serialization.
     */
    public PropertyDefinition() {
	super();
	this.elementType = null;
	this.propertyName = null;
	this.propertyType = null;
	this.uniqueConstraint = null;
    }

    public PropertyDefinition(ElementType elementType, String propertyName, Class<T> propertyType,
	    UniqueConstraint uniqueConstraint) {
	super();
	this.elementType = elementType;
	this.propertyName = propertyName;
	this.propertyType = propertyType;
	this.uniqueConstraint = uniqueConstraint;
    }

    public final ElementType getElementType() {
	return elementType;
    }

    public final String getPropertyName() {
	return propertyName;
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
	result = prime * result + ((propertyName == null) ? 0 : propertyName.hashCode());
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
	if (propertyName == null) {
	    if (other.propertyName != null)
		return false;
	} else if (!propertyName.equals(other.propertyName))
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

}

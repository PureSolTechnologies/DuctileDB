package com.puresoltechnologies.ductiledb.api;

import java.util.Set;

/**
 * This interface defines methods which are common for all elements (vertices
 * and edges) in DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DuctileDBElement {

    /**
     * This method returns the internal id of the element. This id is used to
     * retrieve the element from database.
     * 
     * @return A long value is returned.
     */
    public long getId();

    /**
     * This method returns a list of all property keys of all property currently
     * set on the element. This key can be used with
     * {@link #getProperty(String)} to retrieve the actual value.
     * 
     * @return A {@link Set} of {@link String} is returned containing the keys.
     */
    public Set<String> getPropertyKeys();

    /**
     * This method is used to set a property at the element.
     * 
     * @param key
     *            is the key of the property to be set.
     * @param value
     *            is the value to be set for the property.
     */
    public void setProperty(String key, Object value);

    /**
     * This method is used to read the property value of a specified property.
     * 
     * @param key
     *            is the key of the property to be read.
     * @return The value is returned. <code>null</code> is returned in case the
     *         property is not set on the element.
     */
    public <T> T getProperty(String key);

    /**
     * This method is used to remove a property from the element.
     * 
     * @param key
     *            is the key of the property to be removed.
     * @return
     */
    public <T> T removeProperty(String key);

    public void remove();

}

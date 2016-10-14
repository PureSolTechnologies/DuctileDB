package com.puresoltechnologies.ductiledb.core.graph;

import java.util.Set;

import com.puresoltechnologies.ductiledb.core.graph.tx.DuctileDBTransaction;

/**
 * This interface defines methods which are common for all elements (vertices
 * and edges) in DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DuctileDBElement extends Cloneable {

    /**
     * This method returns the graph which is the source of the current element.
     * 
     * @return A {@link GraphStore} is returned.
     */
    public DuctileDBTransaction getTransaction();

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
     * @param <T>
     *            is the type of the property value.
     */
    public <T> void setProperty(String key, T value);

    /**
     * This method is used to read the property value of a specified property.
     * 
     * @param key
     *            is the key of the property to be read.
     * @param <T>
     *            is the actual type of the property value.
     * @return The value is returned. <code>null</code> is returned in case the
     *         property is not set on the element.
     */
    public <T> T getProperty(String key);

    /**
     * This method is used to remove a property from the element.
     * 
     * @param key
     *            is the key of the property to be removed.
     */
    public void removeProperty(String key);

    /**
     * Removes the element from the graph entirely.
     */
    public void remove();

    /**
     * Clones the object of this element.
     * 
     * @return A {@link DuctileDBElement} is returned.
     */
    public DuctileDBElement clone();
}

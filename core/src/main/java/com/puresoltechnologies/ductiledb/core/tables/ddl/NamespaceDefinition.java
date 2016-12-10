package com.puresoltechnologies.ductiledb.core.tables.ddl;

/**
 * This interface is used to define namespaces.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public interface NamespaceDefinition {

    /**
     * Returns the database name.
     * 
     * @return A {@link String} is returned containing the name of the database.
     */
    public String getDatabaseName();

    /**
     * Returns the namespace name.
     * 
     * @return A {@link String} is returned containing the name of the
     *         namespace.
     */
    public String getName();

}

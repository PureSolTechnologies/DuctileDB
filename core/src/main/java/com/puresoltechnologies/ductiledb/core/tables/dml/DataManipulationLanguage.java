package com.puresoltechnologies.ductiledb.core.tables.dml;

/**
 * This interface provides access to the data manipulation functionality of
 * relational DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DataManipulationLanguage {

    public PreparedInsert preparedInsert(String namespace, String table);

    public PreparedUpdate preparedUpdate(String namespace, String table);

    public PreparedDelete preparedDelete(String namespace, String table);

    public PreparedSelect preparedSelect(String namespace, String table);

    public PreparedTruncate preparedTruncate(String namespace, String table);

}

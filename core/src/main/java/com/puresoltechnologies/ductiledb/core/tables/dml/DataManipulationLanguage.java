package com.puresoltechnologies.ductiledb.core.tables.dml;

/**
 * This interface provides access to the data manipulation functionality of
 * relational DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DataManipulationLanguage {

    public PreparedInsert prepareInsert(String namespace, String table);

    public PreparedUpdate prepareUpdate(String namespace, String table);

    public PreparedDelete prepareDelete(String namespace, String table);

    public PreparedSelect prepareSelect(String namespace, String table);

    public PreparedTruncate prepareTruncate(String namespace, String table);

}

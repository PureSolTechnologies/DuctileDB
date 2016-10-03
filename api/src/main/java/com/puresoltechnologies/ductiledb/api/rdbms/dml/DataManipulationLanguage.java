package com.puresoltechnologies.ductiledb.api.rdbms.dml;

/**
 * This interface provides access to the data manipulation functionality of
 * relational DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DataManipulationLanguage {

    public Insert createInsert(String namespace, String table);

    public Update createUpdate(String namespace, String table);

    public Delete createDelete(String namespace, String table);

    public Select createSelect(String namespace, String table);

    public Truncate createTruncate(String namespace, String table);

}

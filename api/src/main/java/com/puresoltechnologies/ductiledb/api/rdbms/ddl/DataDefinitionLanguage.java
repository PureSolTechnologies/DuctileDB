package com.puresoltechnologies.ductiledb.api.rdbms.ddl;

/**
 * This interface provides access to the data definition functionality of
 * relational DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DataDefinitionLanguage {

    /**
     * This method creates a new {@link CreateNamespace} statement object.
     * 
     * @return
     */
    public CreateNamespace createCreateNamespace(String namespace);

    /**
     * This method creates a new {@link DropNamespace} statement object.
     * 
     * @return
     */
    public DropNamespace createDropNamespace(String namespace);

    /**
     * This method creates a new {@link CreateTable} statement object.
     * 
     * @return
     */
    public CreateTable createCreateTable(String namespace, String table);

    /**
     * This method creates a new {@link DropTable} statement object.
     * 
     * @return
     */
    public DropTable createDropTable(String namespace, String table);

    /**
     * This method creates a new {@link CreateIndex} statement object.
     * 
     * @return
     */
    public CreateIndex createCreateIndex(String namespace, String table, String index);

    /**
     * This method creates a new {@link DropIndex} statement object.
     * 
     * @return
     */
    public DropIndex createDropIndex(String namespace, String table, String index);

}

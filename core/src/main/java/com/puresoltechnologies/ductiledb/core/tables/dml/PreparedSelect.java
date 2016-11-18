package com.puresoltechnologies.ductiledb.core.tables.dml;

/**
 * This interface is used for the SELECT implementation.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface PreparedSelect extends PreparedWhereSelectionStatement {

    /**
     * This method adds a column name
     * 
     * @param column
     * @param alias
     * @return
     */
    public PreparedSelect selectColumn(String column, String alias);

    public default PreparedSelect selectColumn(String column) {
	selectColumn(column, column);
	return this;
    }

}

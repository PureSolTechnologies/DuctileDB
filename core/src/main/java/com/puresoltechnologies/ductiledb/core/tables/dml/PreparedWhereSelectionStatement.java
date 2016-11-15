package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.Collection;

/**
 * This interface represents a prepared statement which has a where selector.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface PreparedWhereSelectionStatement extends PreparedStatement {

    /**
     * This method adds a new where clause placeholder with AND semantics to the
     * {@link PreparedStatement}.
     * 
     * @param column
     *            is the column name to filter.
     * @param index
     *            is the index to be used for a later bind index.
     */
    public void addWherePlaceholder(String columnFamily, String column, CompareOperator operator, int index);

    /**
     * This method adds a new where selection.
     * 
     * @param column
     *            is the column name to filter on.
     * @param value
     *            is the value to look for in the given column.
     */
    public <T extends Comparable<T>> void addWhereSelection(String columnFamily, String column,
	    CompareOperator operator, T value);

    public <T extends Comparable<T>> void addWhereSelection(WhereClause<T> selection);

    public void addWhereSelections(Collection<WhereClause<?>> selections);

}

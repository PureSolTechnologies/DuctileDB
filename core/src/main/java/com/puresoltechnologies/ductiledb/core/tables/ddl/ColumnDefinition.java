package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnTypeDefinition;

/**
 * This interface is used for column definitions which contain information about
 * a single column.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface ColumnDefinition<T> {

    public String getColumnFamily();

    public String getName();

    public String getDescription();

    public ColumnTypeDefinition<T> getType();

}

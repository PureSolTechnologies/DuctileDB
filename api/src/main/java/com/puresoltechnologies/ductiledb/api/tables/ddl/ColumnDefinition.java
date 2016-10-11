package com.puresoltechnologies.ductiledb.api.tables.ddl;

import com.puresoltechnologies.ductiledb.api.tables.columns.ColumnType;

/**
 * This interface is used for column definitions which contain information about
 * a single column.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface ColumnDefinition<T> {

    public String getColumnFamily();

    public String getName();

    public ColumnType<T> getType();

}

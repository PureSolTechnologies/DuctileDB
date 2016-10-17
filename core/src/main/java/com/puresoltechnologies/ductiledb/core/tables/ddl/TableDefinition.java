package com.puresoltechnologies.ductiledb.core.tables.ddl;

import java.util.List;
import java.util.Set;

/**
 * This interface is used for table definitions which explain how a table is
 * defined.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface TableDefinition {

    public String getNamespace();

    public String getName();

    public Set<ColumnDefinition<?>> getColumnDefinitions();

    public List<ColumnDefinition<?>> getPrimaryKey();

    public ColumnDefinition<?> getColumnDefinition(String columnName);

    public boolean isPrimaryKey(ColumnDefinition<?> columnDefinition);

}

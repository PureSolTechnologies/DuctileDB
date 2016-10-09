package com.puresoltechnologies.ductiledb.api.tables.ddl;

import com.puresoltechnologies.ductiledb.api.tables.ValueTypes;

public interface ColumnDefinition {

    public String getColumnFamily();

    public String getName();

    public ValueTypes getType();

}

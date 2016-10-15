package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.core.tables.DuctileDBStatement;

public interface CreateIndex extends DuctileDBStatement {

    public void addColumn(String column);

}

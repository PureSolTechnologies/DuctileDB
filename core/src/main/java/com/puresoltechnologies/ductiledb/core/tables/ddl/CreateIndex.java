package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.core.tables.Statement;

public interface CreateIndex extends Statement {

    public void addColumn(String column);

}

package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.core.tables.PreparedStatement;

public interface CreateIndex extends PreparedStatement {

    public void addColumn(String column);

}

package com.puresoltechnologies.ductiledb.core.tables;

import java.io.Closeable;

import com.puresoltechnologies.ductiledb.core.tables.dcl.DataControlLanguage;
import com.puresoltechnologies.ductiledb.core.tables.ddl.DataDefinitionLanguage;
import com.puresoltechnologies.ductiledb.core.tables.dml.DataManipulationLanguage;

/**
 * This is the central class for the RDBMS functionality of DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface TableStore extends Closeable {

    public DataDefinitionLanguage getDataDefinitionLanguage();

    public DataManipulationLanguage getDataManipulationLanguage();

    public DataControlLanguage getDataControlLanguage();

    public PreparedStatement prepare(String query) throws ExecutionException;
}

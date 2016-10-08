package com.puresoltechnologies.ductiledb.api.tables;

import java.io.Closeable;

import com.puresoltechnologies.ductiledb.api.tables.dcl.DataControlLanguage;
import com.puresoltechnologies.ductiledb.api.tables.ddl.DataDefinitionLanguage;
import com.puresoltechnologies.ductiledb.api.tables.dml.DataManipulationLanguage;

/**
 * This is the central class for the RDBMS functionality of DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface TableStore extends Closeable {

    public DataDefinitionLanguage getDataDefinitionLanguage();

    public DataManipulationLanguage getDataManipulationLanguage();

    public DataControlLanguage getDataControlLanguage();
}

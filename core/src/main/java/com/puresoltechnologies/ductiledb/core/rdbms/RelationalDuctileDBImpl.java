package com.puresoltechnologies.ductiledb.core.rdbms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.api.rdbms.RelationalDuctileDB;
import com.puresoltechnologies.ductiledb.core.rdbms.schema.RelationalSchemaManagerImpl;

/**
 * This is the central class for the RDBMS functionality of DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public class RelationalDuctileDBImpl implements RelationalDuctileDB {

    private static Logger logger = LoggerFactory.getLogger(RelationalDuctileDBImpl.class);

    private final RelationalSchemaManagerImpl relationalSchemaManager = new RelationalSchemaManagerImpl();

}

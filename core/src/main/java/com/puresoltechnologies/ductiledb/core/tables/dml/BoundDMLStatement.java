package com.puresoltechnologies.ductiledb.core.tables.dml;

import com.puresoltechnologies.ductiledb.core.tables.BoundStatement;
import com.puresoltechnologies.ductiledb.core.tables.PreparedStatement;

/**
 * This interface represents a statement which was bound already from a
 * {@link PreparedStatement}.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface BoundDMLStatement extends BoundStatement, DataSettersByName<BoundDMLStatement> {

}

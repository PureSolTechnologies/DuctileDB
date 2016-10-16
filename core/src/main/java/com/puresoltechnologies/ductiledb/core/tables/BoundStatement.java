package com.puresoltechnologies.ductiledb.core.tables;

/**
 * This interface represents a statement which was bound already from a
 * {@link PreparedStatement}.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface BoundStatement
	extends Statement, DataSettersByIndex<BoundStatement>, DataSettersByName<BoundStatement> {

}

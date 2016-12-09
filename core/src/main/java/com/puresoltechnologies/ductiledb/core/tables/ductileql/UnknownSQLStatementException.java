package com.puresoltechnologies.ductiledb.core.tables.ductileql;

import com.puresoltechnologies.parsers.parser.ParseTreeNode;

public class UnknownSQLStatementException extends Exception {

    private static final long serialVersionUID = -6081727043044788019L;

    public UnknownSQLStatementException(ParseTreeNode statement) {
	super("Statement '" + statement.getText() + "' is unknown.");
    }

}

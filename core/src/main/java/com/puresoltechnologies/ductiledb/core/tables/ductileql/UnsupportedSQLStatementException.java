package com.puresoltechnologies.ductiledb.core.tables.ductileql;

import com.puresoltechnologies.parsers.parser.ParseTreeNode;

public class UnsupportedSQLStatementException extends Exception {

    private static final long serialVersionUID = 3878792016673131330L;

    public UnsupportedSQLStatementException(ParseTreeNode statement) {
	super("Statement '" + statement.getText() + "' is unknown.");
    }

}

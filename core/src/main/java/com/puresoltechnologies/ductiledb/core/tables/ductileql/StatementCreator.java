package com.puresoltechnologies.ductiledb.core.tables.ductileql;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.Statement;
import com.puresoltechnologies.ductiledb.core.tables.TableStore;
import com.puresoltechnologies.ductiledb.core.tables.ddl.CreateTable;
import com.puresoltechnologies.ductiledb.core.tables.ddl.DataDefinitionLanguage;
import com.puresoltechnologies.parsers.parser.ParseTreeNode;
import com.puresoltechnologies.trees.TreeException;

/**
 * This class is used to create the DuctileDB statements.
 * 
 * @author Rick-Rainer Ludwig
 */
public class StatementCreator {

    /**
     * Checks whether an SQL statement is a query (having results) or not.
     * 
     * @return
     */
    public static boolean isQuery(ParseTreeNode statement) {
	if ("block-statement".equals(statement.getName())) {
	    /*
	     * block statements cannot have results
	     */
	    return false;
	}
	if (statement.getSubTrees("select-statement").size() > 0) {
	    return true;
	}
	if (statement.getSubTrees("description-statement").size() > 0) {
	    return true;
	}
	return false;
    }

    private final TableStore tableStore;

    public StatementCreator(TableStore tableStore) {
	super();
	this.tableStore = tableStore;
    }

    private String getNamespace(ParseTreeNode tableIdentifier) {
	return tableIdentifier.getChildren().get(0).getToken().getText();
    }

    private String getTable(ParseTreeNode tableIdentifier) {
	return tableIdentifier.getChildren().get(2).getToken().getText();
    }

    public Statement create(ParseTreeNode statement) throws ExecutionException {
	try {
	    if ("block-statement".equals(statement.getName())) {
		return createBlockStatement(statement);
	    }
	    if (statement.getSubTrees("select-statement").size() > 0) {
		return createSingleStatement(statement);
	    }
	    throw new UnknownSQLStatementException(statement);
	} catch (TreeException | UnknownSQLStatementException | UnsupportedSQLStatementException e) {
	    throw new ExecutionException("Statement '" + statement.getText() + "' cannot be created.", e);
	}
    }

    private Statement createBlockStatement(ParseTreeNode blockStatement)
	    throws TreeException, UnknownSQLStatementException, UnsupportedSQLStatementException {
	// for (ParseTreeNode singleStatement :
	// blockStatement.getChildren("statement")) {
	// createSingleStatement(singleStatement);
	// }
	// return null;
	throw new UnsupportedSQLStatementException(blockStatement);
    }

    private Statement createSingleStatement(ParseTreeNode statement)
	    throws UnknownSQLStatementException, TreeException, UnsupportedSQLStatementException {
	ParseTreeNode ddlStatement = statement.getChild("ddl-statement");
	if (ddlStatement != null) {
	    return createDDLStatement(ddlStatement);
	}
	ParseTreeNode dmlStatement = statement.getChild("dml-statement");
	if (dmlStatement != null) {
	    return createDMLStatement(dmlStatement);
	}
	throw new UnknownSQLStatementException(statement);
    }

    private Statement createDDLStatement(ParseTreeNode ddlStatement)
	    throws UnsupportedSQLStatementException, TreeException {
	ParseTreeNode descriptionStatement = ddlStatement.getChild("description-statement");
	if (descriptionStatement != null) {
	    return createDescriptionStatement(descriptionStatement);
	}
	ParseTreeNode tableDefinitionStatement = ddlStatement.getChild("table-definition-statement");
	if (tableDefinitionStatement != null) {
	    return createTableDefinitionStatement(tableDefinitionStatement);
	}
	ParseTreeNode indexDefinitionStatement = ddlStatement.getChild("index-definition-statement");
	if (indexDefinitionStatement != null) {
	    return createIndexDefinitionStatement(indexDefinitionStatement);
	}
	throw new UnsupportedSQLStatementException(ddlStatement);
    }

    private Statement createDescriptionStatement(ParseTreeNode descriptionStatement)
	    throws UnsupportedSQLStatementException {
	// TODO Auto-generated method stub
	throw new UnsupportedSQLStatementException(descriptionStatement);
    }

    private Statement createTableDefinitionStatement(ParseTreeNode tableDefinitionStatement)
	    throws UnsupportedSQLStatementException, TreeException {
	ParseTreeNode createTableStatement = tableDefinitionStatement.getChild("create-table-statement");
	if (createTableStatement != null) {
	    return createCreateTableStatement(createTableStatement);
	}
	ParseTreeNode dropTableStatement = tableDefinitionStatement.getChild("drop-table-statement");
	if (dropTableStatement != null) {
	    return createDropTableStatement(dropTableStatement);
	}
	ParseTreeNode alterTableStatement = tableDefinitionStatement.getChild("alter-table-statement");
	if (alterTableStatement != null) {
	    return createAlterTableStatement(alterTableStatement);
	}
	throw new UnsupportedSQLStatementException(tableDefinitionStatement);
    }

    private Statement createCreateTableStatement(ParseTreeNode createTableStatement) throws TreeException {
	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	ParseTreeNode tableIdentifier = createTableStatement.getChild("table-identifier");
	CreateTable createTable = ddl.createCreateTable(getNamespace(tableIdentifier), getTable(tableIdentifier));
	// TODO Auto-generated method stub
	return createTable;
    }

    private Statement createDropTableStatement(ParseTreeNode dropTableStatement) {
	// TODO Auto-generated method stub
	return null;
    }

    private Statement createAlterTableStatement(ParseTreeNode alterTableStatement) {
	// TODO Auto-generated method stub
	return null;
    }

    private Statement createIndexDefinitionStatement(ParseTreeNode indexDefinitionStatement)
	    throws UnsupportedSQLStatementException {
	// TODO Auto-generated method stub
	throw new UnsupportedSQLStatementException(indexDefinitionStatement);
    }

    private Statement createDMLStatement(ParseTreeNode dmlStatement)
	    throws TreeException, UnknownSQLStatementException, UnsupportedSQLStatementException {
	ParseTreeNode selectStatement = dmlStatement.getChild("select-statement");
	if (selectStatement != null) {
	    return createSelectStatement(selectStatement);
	}
	ParseTreeNode insertStatement = dmlStatement.getChild("insert-statement");
	if (insertStatement != null) {
	    return createInsertStatement(insertStatement);
	}
	ParseTreeNode updateStatement = dmlStatement.getChild("update-statement");
	if (updateStatement != null) {
	    return createUpdateStatement(updateStatement);
	}
	ParseTreeNode deleteStatement = dmlStatement.getChild("delete-statement");
	if (deleteStatement != null) {
	    return createDeleteStatement(deleteStatement);
	}
	throw new UnknownSQLStatementException(dmlStatement);
    }

    private Statement createSelectStatement(ParseTreeNode selectStatement) throws UnsupportedSQLStatementException {
	// TODO Auto-generated method stub
	throw new UnsupportedSQLStatementException(selectStatement);
    }

    private Statement createInsertStatement(ParseTreeNode insertStatement) throws UnsupportedSQLStatementException {
	// TODO Auto-generated method stub
	throw new UnsupportedSQLStatementException(insertStatement);
    }

    private Statement createUpdateStatement(ParseTreeNode updateStatement) throws UnsupportedSQLStatementException {
	// TODO Auto-generated method stub
	throw new UnsupportedSQLStatementException(updateStatement);
    }

    private Statement createDeleteStatement(ParseTreeNode deleteStatement) throws UnsupportedSQLStatementException {
	// TODO Auto-generated method stub
	throw new UnsupportedSQLStatementException(deleteStatement);
    }

}

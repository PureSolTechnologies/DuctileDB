package com.puresoltechnologies.ductiledb.core.tables.sql;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.parsers.parser.ParseTreeNode;

public class SQLParserTest {

    @Test
    public void testGrammarReader() {
	assertNotNull(SQLParser.getGrammar());
    }

    @Test
    public void testDescribeNamespaces() throws ExecutionException {
	ParseTreeNode parseTreeNode = SQLParser.parse("DESCRIBE NAMESPACES;");
	assertNotNull(parseTreeNode);
    }

    @Test
    public void testDescribeNamespace() throws ExecutionException {
	ParseTreeNode parseTreeNode = SQLParser.parse("DESCRIBE NAMESPACE system;");
	assertNotNull(parseTreeNode);
    }

    @Test
    public void testDescribeTable() throws ExecutionException {
	ParseTreeNode parseTreeNode = SQLParser.parse("DESCRIBE TABLE system.namespaces;");
	assertNotNull(parseTreeNode);
    }
}

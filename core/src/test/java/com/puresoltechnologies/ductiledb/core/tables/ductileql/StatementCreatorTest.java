package com.puresoltechnologies.ductiledb.core.tables.ductileql;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.ductileql.SQLParser;
import com.puresoltechnologies.ductiledb.core.tables.ductileql.StatementCreator;
import com.puresoltechnologies.parsers.parser.ParseTreeNode;

public class StatementCreatorTest {

    @Test
    public void testIsQueryDescribeNamespaces() throws ExecutionException {
	ParseTreeNode describeNamespaces = SQLParser.parse("DESCRIBE NAMESPACES;");
	assertTrue(StatementCreator.isQuery(describeNamespaces));
    }

}

package com.puresoltechnologies.ductiledb.core.tables.sql;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class SQLParserTest {

    @Test
    public void testGrammarReader() {
	assertNotNull(SQLParser.getGrammar());
    }

}

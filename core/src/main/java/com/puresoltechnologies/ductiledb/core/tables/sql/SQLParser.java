package com.puresoltechnologies.ductiledb.core.tables.sql;

import java.io.IOException;
import java.io.InputStream;

import com.puresoltechnologies.parsers.grammar.Grammar;
import com.puresoltechnologies.parsers.grammar.GrammarException;
import com.puresoltechnologies.parsers.grammar.GrammarReader;

/**
 * This class is used to parser SQL statements.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class SQLParser {

    private static final Grammar grammar;
    static {
	try (InputStream grammarStream = SQLParser.class.getResourceAsStream("SQL.grammar");
		GrammarReader grammarReader = new GrammarReader(grammarStream);) {
	    grammar = grammarReader.getGrammar();
	} catch (IOException e) {
	    throw new RuntimeException("Could not read grammar.", e);
	} catch (GrammarException e) {
	    throw new RuntimeException("Could not parse grammar.", e);
	}
    }

    static Grammar getGrammar() {
	return grammar;
    }
}

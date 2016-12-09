package com.puresoltechnologies.ductiledb.core.tables.ductileql;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.parsers.analyzer.AnalyzerFactory;
import com.puresoltechnologies.parsers.grammar.Grammar;
import com.puresoltechnologies.parsers.grammar.GrammarException;
import com.puresoltechnologies.parsers.grammar.GrammarReader;
import com.puresoltechnologies.parsers.lexer.Lexer;
import com.puresoltechnologies.parsers.lexer.LexerException;
import com.puresoltechnologies.parsers.lexer.TokenStream;
import com.puresoltechnologies.parsers.parser.ParseTreeNode;
import com.puresoltechnologies.parsers.parser.Parser;
import com.puresoltechnologies.parsers.parser.ParserException;
import com.puresoltechnologies.parsers.source.SourceCode;
import com.puresoltechnologies.parsers.source.UnspecifiedSourceCodeLocation;

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

    public static ParseTreeNode parse(String statement) throws ExecutionException {
	try {
	    Grammar grammar = getGrammar();
	    AnalyzerFactory factory = AnalyzerFactory.createFactory(grammar, SQLParser.class.getClassLoader());
	    Lexer lexer = factory.createLexer();
	    TokenStream tokenStream = lexer.lex(SourceCode.read(new ByteArrayInputStream(statement.getBytes()),
		    new UnspecifiedSourceCodeLocation()));
	    Parser parser = factory.createParser();
	    return parser.parse(tokenStream);
	} catch (GrammarException | LexerException | IOException | ParserException e) {
	    throw new ExecutionException("Could not parse statement '" + statement + "'.", e);
	}
    }
}

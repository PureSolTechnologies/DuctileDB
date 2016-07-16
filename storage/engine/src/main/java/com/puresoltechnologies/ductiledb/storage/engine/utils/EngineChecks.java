package com.puresoltechnologies.ductiledb.storage.engine.utils;

import java.util.regex.Pattern;

/**
 * This class contains helper methods to check for certain conditions within the
 * storage engine.
 * 
 * @author Rick-Rainer Ludwig
 */
public class EngineChecks {

    public static final String IDENTIFIED_FORM = "[a-zA-Z][-_a-zA-Z0-9]*";
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile(IDENTIFIED_FORM);

    public static boolean checkIdentifier(String identifier) {
	if (identifier == null) {
	    return false;
	}
	if (IDENTIFIER_PATTERN.matcher(identifier).matches()) {
	    return true;
	} else {
	    return false;
	}
    }
}

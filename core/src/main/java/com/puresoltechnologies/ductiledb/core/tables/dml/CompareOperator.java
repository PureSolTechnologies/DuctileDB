package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.function.Predicate;

/**
 * This is the enum containing the possible operators for comparison in where
 * clauses.
 * 
 * @author Rick-Rainer Ludwig
 */
public enum CompareOperator {

    EQUALS(c -> c == 0), //
    LESS_THEN(c -> c < 0), //
    LESS_OR_EQUAL(c -> c <= 0), //
    GREATER_THAN(c -> c > 0), //
    GREATER_OR_EQUAL(c -> c >= 0);

    private final Predicate<Integer> filter;

    private CompareOperator(Predicate<Integer> filter) {
	this.filter = filter;
    }

    public <T extends Comparable<T>> boolean matches(T reference, T value) {
	return filter.test(reference.compareTo(value));
    }

}

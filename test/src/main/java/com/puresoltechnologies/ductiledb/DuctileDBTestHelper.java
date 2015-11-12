package com.puresoltechnologies.ductiledb;

/**
 * A collection of simple methods to support testing.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBTestHelper {

    /**
     * Runs through an {@link Iterable} and counts the number of elements.
     * 
     * @param iterable
     *            is the {@link Iterable} to count the elements in.
     * @return
     */
    public static long count(Iterable<?> iterable) {
	long[] count = { 0 };
	iterable.forEach(c -> count[0]++);
	return count[0];
    }

}

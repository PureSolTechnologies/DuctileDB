package com.puresoltechnologies.ductiledb.core.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.core.utils.EmptyIterable;

public class EmptyIterableTest {

    @Test
    public void test() {
	EmptyIterable<Byte> iterable = new EmptyIterable<>();
	Iterator<Byte> iterator = iterable.iterator();
	assertNotNull(iterator);
	assertFalse(iterator.hasNext());
	try {
	    assertNull(iterator.next());
	    fail("NoSuchElementException was exptect.");
	} catch (NoSuchElementException e) {
	    // intentionally left empty
	}
    }

}

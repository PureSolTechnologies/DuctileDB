package com.puresoltechnologies.ductiledb.core.utils;

import java.util.Collections;
import java.util.Iterator;

public class EmptyIterable<T> implements Iterable<T> {

    @Override
    public Iterator<T> iterator() {
	return Collections.emptyIterator();
    }

}

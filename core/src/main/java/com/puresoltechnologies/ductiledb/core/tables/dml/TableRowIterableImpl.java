package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;

import com.puresoltechnologies.commons.misc.io.CloseableIterable;

public class TableRowIterableImpl<T> implements TableRowIterable {

    private final CloseableIterable<T> sourceIterable;
    private final Function<T, TableRow> converter;

    public TableRowIterableImpl(Iterable<T> sourceIterable, Function<T, TableRow> converter) {
	super();
	this.sourceIterable = new CloseableIterable<T>() {

	    @Override
	    public void close() throws IOException {
		// intentionally left black
	    }

	    @Override
	    public Iterator<T> iterator() {
		return sourceIterable.iterator();
	    }
	};
	this.converter = converter;
    }

    public TableRowIterableImpl(CloseableIterable<T> sourceIterable, Function<T, TableRow> converter) {
	super();
	this.sourceIterable = sourceIterable;
	this.converter = converter;
    }

    @Override
    public Iterator<TableRow> iterator() {
	return new Iterator<TableRow>() {

	    private final Iterator<T> iterator = sourceIterable.iterator();

	    @Override
	    public boolean hasNext() {
		return iterator.hasNext();
	    }

	    @Override
	    public TableRow next() {
		return converter.apply(iterator.next());
	    }
	};
    }

    @Override
    public void close() throws IOException {
	sourceIterable.close();
    }

}

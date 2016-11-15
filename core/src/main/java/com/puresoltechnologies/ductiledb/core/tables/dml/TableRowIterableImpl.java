package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;

import com.puresoltechnologies.commons.misc.io.AbstractPeekingCloseableIterator;
import com.puresoltechnologies.commons.misc.io.CloseableIterable;

/**
 * This is an implementation of {@link TableRowIterable} to wrap easily other
 * {@link Iterable}s or {@link CloseableIterable}s into row iterable.
 * 
 * @author Rick-Rainer Ludwig
 *
 * @param <T>
 */
public class TableRowIterableImpl<T> implements TableRowIterable {

    private final CloseableIterable<T> sourceIterable;
    private final Function<T, TableRow> converter;
    private final Predicate<TableRow> filter;

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
	this.filter = null;
    }

    public TableRowIterableImpl(CloseableIterable<T> sourceIterable, Function<T, TableRow> converter) {
	super();
	this.sourceIterable = sourceIterable;
	this.converter = converter;
	this.filter = null;
    }

    public TableRowIterableImpl(CloseableIterable<T> sourceIterable, Function<T, TableRow> converter,
	    Predicate<TableRow> filter) {
	super();
	this.sourceIterable = sourceIterable;
	this.converter = converter;
	this.filter = filter;
    }

    @Override
    public Iterator<TableRow> iterator() {
	return new AbstractPeekingCloseableIterator<TableRow>() {

	    private final Iterator<T> iterator = sourceIterable.iterator();

	    @Override
	    public void close() throws IOException {
		// intentionally left empty
	    }

	    @Override
	    protected TableRow readNext() {
		while (iterator.hasNext()) {
		    T next = iterator.next();
		    if (next == null) {
			return null;
		    }
		    TableRow row = converter.apply(next);
		    if (filter.test(row)) {
			return row;
		    }
		}
		return null;
	    }

	};
    }

    @Override
    public void close() throws IOException {
	sourceIterable.close();
    }

}

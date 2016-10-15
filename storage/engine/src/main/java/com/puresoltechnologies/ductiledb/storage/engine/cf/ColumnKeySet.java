package com.puresoltechnologies.ductiledb.storage.engine.cf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * This is a set of column identifiers. These are sorted in the order of
 * retrieval.
 * 
 * @author Rick-Rainer Ludwig
 */
public class ColumnKeySet implements Set<byte[]> {

    private static final long serialVersionUID = 2126693320865897642L;

    private final List<byte[]> columns = new ArrayList<>();

    public ColumnKeySet() {
	super();
    }

    public ColumnKeySet(byte[]... columns) {
	this();
	for (byte[] column : columns) {
	    add(column);
	}
    }

    public ColumnKeySet(Set<byte[]> columns) {
	this();
	for (byte[] column : columns) {
	    add(column);
	}
    }

    @Override
    public int size() {
	return columns.size();
    }

    @Override
    public boolean isEmpty() {
	return columns.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
	if (!o.getClass().isArray()) {
	    return false;
	}
	if (!byte.class.equals(o.getClass().getComponentType())) {
	    return false;
	}
	for (byte[] column : columns) {
	    if (Arrays.equals(column, (byte[]) o)) {
		return true;
	    }
	}
	return false;
    }

    @Override
    public Iterator<byte[]> iterator() {
	return columns.iterator();
    }

    @Override
    public Object[] toArray() {
	return columns.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
	return columns.toArray(a);
    }

    @Override
    public boolean add(byte[] e) {
	if (contains(e)) {
	    return false;
	}
	return columns.add(e);
    }

    @Override
    public boolean remove(Object o) {
	if (!o.getClass().isArray()) {
	    return false;
	}
	if (!byte.class.equals(o.getClass().getComponentType())) {
	    return false;
	}
	Iterator<byte[]> iterator = columns.iterator();
	while (iterator.hasNext()) {
	    if (Arrays.equals((byte[]) o, iterator.next())) {
		iterator.remove();
		return true;
	    }
	}
	return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
	for (Object e : c) {
	    if (!contains(e)) {
		return false;
	    }
	}
	return true;
    }

    @Override
    public boolean addAll(Collection<? extends byte[]> c) {
	return columns.addAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
	Iterator<byte[]> iterator = columns.iterator();
	boolean changed = false;
	while (iterator.hasNext()) {
	    if (!c.contains(iterator.next())) {
		iterator.remove();
		changed = true;
	    }
	}
	return changed;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
	Iterator<byte[]> iterator = columns.iterator();
	boolean changed = false;
	while (iterator.hasNext()) {
	    if (c.contains(iterator.next())) {
		iterator.remove();
		changed = true;
	    }
	}
	return changed;
    }

    @Override
    public void clear() {
	columns.clear();
    }

}

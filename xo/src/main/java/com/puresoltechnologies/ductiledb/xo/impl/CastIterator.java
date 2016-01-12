package com.puresoltechnologies.ductiledb.xo.impl;

import java.util.Iterator;

public class CastIterator<FROM, TO> implements Iterator<TO> {

    private final Iterator<FROM> fromIterator;

    public CastIterator(Iterator<FROM> fromIterator) {
	super();
	this.fromIterator = fromIterator;
    }

    @Override
    public boolean hasNext() {
	return fromIterator.hasNext();
    }

    @Override
    public TO next() {
	@SuppressWarnings("unchecked")
	TO to = (TO) fromIterator.next();
	return to;
    }

}

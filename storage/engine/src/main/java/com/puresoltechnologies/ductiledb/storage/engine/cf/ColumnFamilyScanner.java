package com.puresoltechnologies.ductiledb.storage.engine.cf;

import java.io.Closeable;

import com.puresoltechnologies.commons.misc.PeekingIterator;

/**
 * This interface is used for all Column Family scanners. These scanners may run
 * over row keys, secondary indexes or compound index if implemented.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface ColumnFamilyScanner extends PeekingIterator<ColumnFamilyRow>, Closeable {

    @Override
    public ColumnFamilyRow peek();

    @Override
    public ColumnFamilyRow next();

}

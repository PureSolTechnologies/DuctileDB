package com.puresoltechnologies.ductiledb.bigtable.cf;

import java.io.Closeable;

import com.puresoltechnologies.commons.misc.PeekingIterator;

public interface ColumnFamilyScanner extends PeekingIterator<ColumnFamilyRow>, Closeable {

}

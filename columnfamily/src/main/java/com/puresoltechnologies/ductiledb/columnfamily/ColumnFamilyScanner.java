package com.puresoltechnologies.ductiledb.columnfamily;

import java.io.Closeable;

import com.puresoltechnologies.commons.misc.PeekingIterator;

public interface ColumnFamilyScanner extends PeekingIterator<ColumnFamilyRow>, Closeable {

}

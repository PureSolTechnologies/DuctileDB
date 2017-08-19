package com.puresoltechnologies.ductiledb.columnfamily;

import java.io.Closeable;

import com.puresoltechnologies.streaming.StreamIterator;

public interface ColumnFamilyScanner extends StreamIterator<ColumnFamilyRow>, Closeable {

    default public void skip() {
	next();
    }

}

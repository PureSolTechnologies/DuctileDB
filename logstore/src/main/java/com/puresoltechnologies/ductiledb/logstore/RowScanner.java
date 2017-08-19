package com.puresoltechnologies.ductiledb.logstore;

import java.io.Closeable;

import com.puresoltechnologies.streaming.StreamIterator;

/**
 * This interface is used for all Column Family scanners. These scanners may run
 * over row keys, secondary indexes or compound index if implemented.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface RowScanner extends StreamIterator<Row>, Closeable {

}

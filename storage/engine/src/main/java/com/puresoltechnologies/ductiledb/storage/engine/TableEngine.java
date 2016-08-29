package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.Closeable;

import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyEngine;

/**
 * This class is the central engine class for table storage. It is using the
 * {@link ColumnFamilyEngine} to store the separated column families.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface TableEngine extends Closeable {

}

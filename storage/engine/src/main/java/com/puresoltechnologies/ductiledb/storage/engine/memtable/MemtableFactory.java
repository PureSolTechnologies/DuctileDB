package com.puresoltechnologies.ductiledb.storage.engine.memtable;

/**
 * This method create memory tables based on configuration.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class MemtableFactory {

    public static Memtable create() {
	return new MemtableImpl();
    }

}

package com.puresoltechnologies.ductiledb.storage.engine.memtable;

import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;

/**
 * This method create memory tables based on configuration.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class MemtableFactory {

    public static Memtable create(TableDescriptor tableDescriptor) {
	return new MemtableImpl(tableDescriptor);
    }

}

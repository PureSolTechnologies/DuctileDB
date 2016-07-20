package com.puresoltechnologies.ductiledb.storage.engine.memtable;

import java.util.TreeMap;

import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;

/**
 * This map is used to store columns and their values. The underlying map is a
 * {@link TreeMap} sorting the keys for SSTable files.
 * 
 * @author Rick-Rainer Ludwig
 */
public class ColumnMap extends TreeMap<byte[], byte[]> {

    private static final long serialVersionUID = 5541936926822843626L;

    public ColumnMap() {
	super(ByteArrayComparator.getInstance());
    }

}

package com.puresoltechnologies.ductiledb.storage.engine.memtable;

import java.util.TreeMap;

import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;

/**
 * This map is used to store rows and their columns. The underlying map is a
 * {@link TreeMap} sorting the keys for SSTable files.
 * 
 * @author Rick-Rainer Ludwig
 */
public class RowMap extends TreeMap<byte[], ColumnMap> {

    private static final long serialVersionUID = -1297395185107292980L;

    public RowMap() {
	super(ByteArrayComparator.getInstance());
    }

}

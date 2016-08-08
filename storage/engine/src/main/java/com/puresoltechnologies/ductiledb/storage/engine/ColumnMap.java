package com.puresoltechnologies.ductiledb.storage.engine;

import java.util.Map;
import java.util.TreeMap;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
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

    public ColumnMap(Map<byte[], byte[]> columnFamily) {
	this();
	putAll(columnFamily);
    }

    @Override
    public String toString() {
	StringBuffer buffer = new StringBuffer();
	for (java.util.Map.Entry<byte[], byte[]> entry : entrySet()) {
	    buffer.append(Bytes.toHumanReadableString(entry.getKey()));
	    buffer.append("=");
	    buffer.append(Bytes.toHumanReadableString(entry.getValue()));
	    buffer.append("\n");
	}
	return buffer.toString();
    }

    @Override
    public int hashCode() {
	// intentionally constant.
	return 42;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	ColumnMap other = (ColumnMap) obj;
	if (size() != other.size()) {
	    return false;
	}
	ByteArrayComparator comparator = ByteArrayComparator.getInstance();
	for (java.util.Map.Entry<byte[], byte[]> entry : entrySet()) {
	    if (comparator.compare(entry.getValue(), other.get(entry.getKey())) != 0) {
		return false;
	    }
	}
	return true;
    }

}

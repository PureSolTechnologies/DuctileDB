package com.puresoltechnologies.ductiledb.storage.engine.schema;

import java.io.File;
import java.util.TreeMap;

import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;

public class TableDescriptor {

    private final TreeMap<byte[], ColumnFamilyDescriptor> columnFamilies = new TreeMap<>(
	    ByteArrayComparator.getInstance());
    private final String name;
    private final NamespaceDescriptor namespace;
    private final File directory;

    public TableDescriptor(NamespaceDescriptor namespace, String name) {
	this.namespace = namespace;
	this.name = name;
	this.directory = new File(namespace.getDirectory(), name);
    }

    public final String getName() {
	return name;
    }

    public final NamespaceDescriptor getNamespace() {
	return namespace;
    }

    public final File getDirectory() {
	return directory;
    }

    @Override
    public String toString() {
	return "table:" + namespace.getName() + "." + name;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((directory == null) ? 0 : directory.hashCode());
	result = prime * result + ((name == null) ? 0 : name.hashCode());
	result = prime * result + ((namespace == null) ? 0 : namespace.hashCode());
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	TableDescriptor other = (TableDescriptor) obj;
	if (directory == null) {
	    if (other.directory != null)
		return false;
	} else if (!directory.equals(other.directory))
	    return false;
	if (name == null) {
	    if (other.name != null)
		return false;
	} else if (!name.equals(other.name))
	    return false;
	if (namespace == null) {
	    if (other.namespace != null)
		return false;
	} else if (!namespace.equals(other.namespace))
	    return false;
	return true;
    }

    public void addColumnFamily(ColumnFamilyDescriptor columnFamilyDescriptor) {
	columnFamilies.put(columnFamilyDescriptor.getName(), columnFamilyDescriptor);
    }

    public void removeColumnFamily(ColumnFamilyDescriptor columnFamilyDescriptor) {
	columnFamilies.remove(columnFamilyDescriptor.getName());
    }

    public Iterable<ColumnFamilyDescriptor> getColumnFamilies() {
	return columnFamilies.values();
    }

    public ColumnFamilyDescriptor getColumnFamily(byte[] columnFamilyName) {
	return columnFamilies.get(columnFamilyName);
    }

}

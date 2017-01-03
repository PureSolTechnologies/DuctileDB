package com.puresoltechnologies.ductiledb.bigtable;

import java.io.File;
import java.util.TreeMap;

import com.puresoltechnologies.ductiledb.bigtable.cf.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.logstore.Key;

public class TableDescriptor {

    private final TreeMap<Key, ColumnFamilyDescriptor> columnFamilies = new TreeMap<>();
    private final NamespaceDescriptor namespace;
    private final String name;
    private final String description;
    private final File directory;

    public TableDescriptor(NamespaceDescriptor namespace, String name, String description) {
	this.namespace = namespace;
	this.name = name;
	this.description = description;
	this.directory = new File(namespace.getDirectory(), name);
    }

    public final NamespaceDescriptor getNamespace() {
	return namespace;
    }

    public final String getName() {
	return name;
    }

    public final String getDescription() {
	return description;
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

    public ColumnFamilyDescriptor getColumnFamily(Key columnFamilyName) {
	return columnFamilies.get(columnFamilyName);
    }

}

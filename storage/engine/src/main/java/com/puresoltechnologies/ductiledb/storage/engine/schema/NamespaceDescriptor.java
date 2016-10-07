package com.puresoltechnologies.ductiledb.storage.engine.schema;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class NamespaceDescriptor {

    private final Map<String, TableDescriptor> tables = new HashMap<>();
    private final String name;
    private final Storage storage;
    private final File directory;

    /**
     * This is the primary constructor for the namespace. The namespace name is
     * determined by {@link File#getName()} of the namespace directory.
     * 
     * @param storage
     *            is the {@link Storage} to be used to put the data to.
     * @param directory
     *            is the directory in the storage to be used to put data into
     *            which belongs to the namespace.
     */
    public NamespaceDescriptor(Storage storage, File directory) {
	this.storage = storage;
	this.directory = directory;
	this.name = directory.getName();
    }

    public final String getName() {
	return name;
    }

    public final File getDirectory() {
	return directory;
    }

    @Override
    public String toString() {
	return "namespace:" + name;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((directory == null) ? 0 : directory.hashCode());
	result = prime * result + ((name == null) ? 0 : name.hashCode());
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
	NamespaceDescriptor other = (NamespaceDescriptor) obj;
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
	return true;
    }

    public void addTable(TableDescriptor tableDescriptor) {
	tables.put(tableDescriptor.getName(), tableDescriptor);
    }

    public void removeTable(TableDescriptor tableDescriptor) {
	tables.remove(tableDescriptor.getName());
    }

    public Iterable<TableDescriptor> getTables() {
	return tables.values();
    }

    public TableDescriptor getTable(String tableName) {
	return tables.get(tableName);
    }

}

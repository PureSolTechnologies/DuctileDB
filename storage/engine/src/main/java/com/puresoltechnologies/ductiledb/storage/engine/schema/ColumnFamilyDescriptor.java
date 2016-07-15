package com.puresoltechnologies.ductiledb.storage.engine.schema;

import java.io.File;

public class ColumnFamilyDescriptor {

    private final String name;
    private final TableDescriptor table;
    private final File directory;

    public ColumnFamilyDescriptor(String name, TableDescriptor table, File directory) {
	this.name = name;
	this.table = table;
	this.directory = directory;
    }

    public final String getName() {
	return name;
    }

    public final File getDirectory() {
	return directory;
    }

    @Override
    public String toString() {
	return name;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((directory == null) ? 0 : directory.hashCode());
	result = prime * result + ((name == null) ? 0 : name.hashCode());
	result = prime * result + ((table == null) ? 0 : table.hashCode());
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
	ColumnFamilyDescriptor other = (ColumnFamilyDescriptor) obj;
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
	if (table == null) {
	    if (other.table != null)
		return false;
	} else if (!table.equals(other.table))
	    return false;
	return true;
    }

}

package com.puresoltechnologies.ductiledb.columnfamily;

import java.io.File;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.puresoltechnologies.ductiledb.logstore.Key;

public class ColumnFamilyDescriptor {

    private final Key name;
    private final File directory;
    private final File indexDirectory;

    @JsonCreator
    public ColumnFamilyDescriptor(//
	    @JsonProperty("name") Key name, //
	    @JsonProperty("directory") File directory) {
	this.name = name;
	this.directory = directory;
	this.indexDirectory = new File(directory, "indizes");
	if (!directory.isAbsolute()) {
	    throw new IllegalArgumentException("An absolute path to directory is needed.");
	}
    }

    public final Key getName() {
	return name;
    }

    public final File getDirectory() {
	return directory;
    }

    @JsonIgnore
    public final File getIndexDirectory() {
	return indexDirectory;
    }

    @Override
    public String toString() {
	return "column family:" + name;
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
	return true;
    }

}

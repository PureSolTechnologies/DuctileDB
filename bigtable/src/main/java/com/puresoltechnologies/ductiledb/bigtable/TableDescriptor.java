package com.puresoltechnologies.ductiledb.bigtable;

import java.io.File;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TableDescriptor {

    private final String name;
    private final String description;
    private final File directory;

    @JsonCreator
    public TableDescriptor(//
	    @JsonProperty("name") String name, //
	    @JsonProperty("descriptor") String description, //
	    @JsonProperty("directory") File directory) {
	this.name = name;
	this.description = description;
	this.directory = directory;
	if (!directory.isAbsolute()) {
	    throw new IllegalArgumentException("An absolute path to directory is needed.");
	}
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
	return "table:" + name;
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
	return true;
    }
}

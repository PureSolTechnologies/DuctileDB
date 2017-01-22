package com.puresoltechnologies.ductiledb.engine;

import java.io.File;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class NamespaceDescriptor {

    private final String name;
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
    @JsonCreator
    public NamespaceDescriptor(@JsonProperty("directory") File directory) {
	this.directory = directory;
	this.name = directory.getName();
	if (!directory.isAbsolute()) {
	    throw new IllegalArgumentException("An absolute path to directory is needed.");
	}
    }

    @JsonIgnore
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

}

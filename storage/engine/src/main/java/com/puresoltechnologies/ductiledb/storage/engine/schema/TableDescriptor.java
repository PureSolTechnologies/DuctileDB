package com.puresoltechnologies.ductiledb.storage.engine.schema;

import java.io.File;

public class TableDescriptor {

    private final String name;
    private final NamespaceDescriptor namespace;
    private final File directory;

    public TableDescriptor(String name, NamespaceDescriptor namespace, File directory) {
	this.name = name;
	this.namespace = namespace;
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

}

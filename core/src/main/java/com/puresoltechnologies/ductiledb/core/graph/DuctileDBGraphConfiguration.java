package com.puresoltechnologies.ductiledb.core.graph;

public class DuctileDBGraphConfiguration {

    private String namespace = "ductiledb";

    public String getNamespace() {
	return namespace;
    }

    public void setNamespace(String namespace) {
	this.namespace = namespace;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
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
	DuctileDBGraphConfiguration other = (DuctileDBGraphConfiguration) obj;
	if (namespace == null) {
	    if (other.namespace != null)
		return false;
	} else if (!namespace.equals(other.namespace))
	    return false;
	return true;
    }

}

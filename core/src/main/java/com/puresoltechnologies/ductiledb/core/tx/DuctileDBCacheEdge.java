package com.puresoltechnologies.ductiledb.core.tx;

import java.util.Map;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;

class DuctileDBCacheEdge extends DuctileDBCacheElement implements DuctileDBEdge {

    private final String type;
    private final long startVertexId;
    private final long targetVertexId;

    public DuctileDBCacheEdge(DuctileDBGraphImpl graph, long id, String type, long startVertexId, long targetVertexId,
	    Map<String, Object> properties) {
	super(graph, id, properties);
	if (type == null) {
	    throw new IllegalArgumentException("Type must not be null.");
	}
	this.type = type;
	this.startVertexId = startVertexId;
	this.targetVertexId = targetVertexId;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = super.hashCode();
	result = prime * result + ((type == null) ? 0 : type.hashCode());
	result = prime * result + (int) (startVertexId ^ (startVertexId >>> 32));
	result = prime * result + (int) (targetVertexId ^ (targetVertexId >>> 32));
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (!super.equals(obj))
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	DuctileDBCacheEdge other = (DuctileDBCacheEdge) obj;
	if (type == null) {
	    if (other.type != null)
		return false;
	} else if (!type.equals(other.type))
	    return false;
	if (startVertexId != other.startVertexId)
	    return false;
	if (targetVertexId != other.targetVertexId)
	    return false;
	return true;
    }

    @Override
    public DuctileDBVertex getStartVertex() {
	throw new IllegalStateException("This method is not provided for cached elements.");
    }

    public long getStartVertexId() {
	return startVertexId;
    }

    @Override
    public DuctileDBVertex getTargetVertex() {
	throw new IllegalStateException("This method is not provided for cached elements.");
    }

    public long getTargetVertexId() {
	return targetVertexId;
    }

    @Override
    public String getType() {
	return type;
    }

    @Override
    public void remove() {
	getGraph().removeEdge(this);
    }

    @Override
    public String toString() {
	return getClass().getSimpleName() + " " + getId() + " (" + startVertexId + "->" + targetVertexId + "): type="
		+ type + "; properties=" + getPropertiesString();
    }

    @Override
    public DuctileDBCacheEdge clone() {
	DuctileDBCacheEdge cloned = (DuctileDBCacheEdge) super.clone();
	ElementUtils.setFinalField(cloned, DuctileDBCacheEdge.class, "type", type);
	ElementUtils.setFinalField(cloned, DuctileDBCacheEdge.class, "startVertexId", startVertexId);
	ElementUtils.setFinalField(cloned, DuctileDBCacheEdge.class, "targetVertexId", targetVertexId);
	ElementUtils.setFinalField(cloned, DuctileDBCacheEdge.class, "startVertex", null);
	ElementUtils.setFinalField(cloned, DuctileDBCacheEdge.class, "targetVertex", null);
	return cloned;
    }
}

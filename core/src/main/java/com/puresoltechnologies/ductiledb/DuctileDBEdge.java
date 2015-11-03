package com.puresoltechnologies.ductiledb;

import com.tinkerpop.blueprints.Edge;

public interface DuctileDBEdge extends Edge {

    public DuctileDBVertex getStartVertex();

    public DuctileDBVertex getTargetVertex();

    @Override
    public Long getId();

}

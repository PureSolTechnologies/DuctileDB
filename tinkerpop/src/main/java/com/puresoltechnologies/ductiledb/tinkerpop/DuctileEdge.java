package com.puresoltechnologies.ductiledb.tinkerpop;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class DuctileEdge extends DuctileElement implements Edge {

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
	// TODO Auto-generated method stub
	return null;
    }

}

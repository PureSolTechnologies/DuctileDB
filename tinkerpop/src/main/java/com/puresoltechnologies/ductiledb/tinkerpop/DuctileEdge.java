package com.puresoltechnologies.ductiledb.tinkerpop;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;

public class DuctileEdge extends DuctileElement implements Edge, WrappedEdge<DuctileDBEdge> {

    private final DuctileDBEdge baseEdge;

    public DuctileEdge(DuctileDBEdge baseEdge, DuctileGraph graph) {
	super(baseEdge.getId(), graph);
	this.baseEdge = baseEdge;
    }

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

    @Override
    public DuctileDBEdge getBaseEdge() {
	return baseEdge;
    }

    @Override
    public String label() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public void remove() {
	// TODO Auto-generated method stub

    }

}

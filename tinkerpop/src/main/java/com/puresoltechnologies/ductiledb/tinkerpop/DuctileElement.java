package com.puresoltechnologies.ductiledb.tinkerpop;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

public abstract class DuctileElement implements Element {

    private final long id;
    private final DuctileGraph graph;

    public DuctileElement(long id, DuctileGraph graph) {
	super();
	this.id = id;
	this.graph = graph;
    }

    @Override
    public final Long id() {
	return id;
    }

    @Override
    public final DuctileGraph graph() {
	return graph;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
	// TODO Auto-generated method stub
	return null;
    }

}

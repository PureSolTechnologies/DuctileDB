package com.puresoltechnologies.ductiledb.tinkerpop;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;

public abstract class DuctileElement implements Element {

    @Override
    public Object id() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public String label() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Graph graph() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public void remove() {
	// TODO Auto-generated method stub

    }

}

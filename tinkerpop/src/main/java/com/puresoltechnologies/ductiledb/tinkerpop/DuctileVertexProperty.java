package com.puresoltechnologies.ductiledb.tinkerpop;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

public class DuctileVertexProperty<V> implements VertexProperty<V> {

    @Override
    public String key() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public V value() throws NoSuchElementException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public boolean isPresent() {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public void remove() {
	// TODO Auto-generated method stub

    }

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
    public <U> Property<U> property(String key, U value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Vertex element() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
	// TODO Auto-generated method stub
	return null;
    }

}

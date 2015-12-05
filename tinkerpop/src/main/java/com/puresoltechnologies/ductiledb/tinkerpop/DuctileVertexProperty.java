package com.puresoltechnologies.ductiledb.tinkerpop;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

public class DuctileVertexProperty<V> implements VertexProperty<V> {

    private final DuctileVertex vertex;
    private final String key;
    private final V value;

    public DuctileVertexProperty(DuctileVertex vertex, String key, V value) {
	this.vertex = vertex;
	this.key = key;
	this.value = value;
    }

    @Override
    public String key() {
	return key;
    }

    @Override
    public V value() throws NoSuchElementException {
	return value;
    }

    @Override
    public boolean isPresent() {
	return value != null;
    }

    @Override
    public void remove() {
	// TODO Auto-generated method stub

    }

    @Override
    public Long id() {
	return vertex.id();
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
    public DuctileVertex element() {
	return vertex;
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
	vertex.graph().tx().readWrite();
	List<Property<U>> properties = new ArrayList<>();
	for (String propertyKey : propertyKeys) {
	    @SuppressWarnings("unchecked")
	    U propertyValue = (U) vertex.getBaseVertex().getProperty(propertyKey);
	    properties.add(new DuctileProperty<>(vertex, propertyKey, propertyValue));
	}
	return properties.iterator();
    }

}

package com.puresoltechnologies.ductiledb.tinkerpop;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

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
	graph().tx().readWrite();
	vertex.getBaseVertex().removeProperty(key);
    }

    @Override
    public Long id() {
	return (long) (key.hashCode() + value.hashCode() + vertex.id().hashCode());
    }

    @Override
    public String label() {
	return key;
    }

    @Override
    public Graph graph() {
	return vertex.graph();
    }

    @Override
    public <U> Property<U> property(String key, U value) {
	throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public DuctileVertex element() {
	return vertex;
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
	throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public boolean equals(final Object object) {
	return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
	return ElementHelper.hashCode((Element) this);
    }

}

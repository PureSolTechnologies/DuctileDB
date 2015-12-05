package com.puresoltechnologies.ductiledb.tinkerpop;

import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

import com.puresoltechnologies.ductiledb.api.DuctileDBElement;

public class DuctileProperty<V> implements Property<V> {

    protected final Element element;
    protected final String key;
    protected final DuctileGraph graph;
    protected V value;
    protected boolean removed = false;

    public DuctileProperty(DuctileElement element, String key, V value) {
	super();
	this.element = element;
	this.key = key;
	this.graph = element.graph();
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
    public Element element() {
	return element;
    }

    @Override
    public void remove() {
	if (this.removed)
	    return;
	this.removed = true;
	this.graph.tx().readWrite();
	@SuppressWarnings("unchecked")
	final DuctileDBElement entity = this.element instanceof DuctileVertexProperty
		? ((DuctileVertexProperty<V>) this.element).element().getBaseElement()
		: ((DuctileElement) this.element).getBaseElement();
	if (entity.getProperty(key) != null) {
	    entity.removeProperty(key);
	}
    }

}

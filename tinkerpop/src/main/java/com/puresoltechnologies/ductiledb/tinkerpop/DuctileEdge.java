package com.puresoltechnologies.ductiledb.tinkerpop;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;

public class DuctileEdge extends DuctileElement implements Edge, WrappedEdge<DuctileDBEdge> {

    private final DuctileDBEdge baseEdge;

    public DuctileEdge(DuctileDBEdge baseEdge, DuctileGraph graph) {
	super(baseEdge, graph);
	this.baseEdge = baseEdge;
    }

    @Override
    public Vertex outVertex() {
	return new DuctileVertex(baseEdge.getStartVertex(), graph());
    }

    @Override
    public Vertex inVertex() {
	return new DuctileVertex(baseEdge.getTargetVertex(), graph());
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
	switch (direction) {
	case OUT:
	    return IteratorUtils.of(new DuctileVertex(this.getBaseEdge().getStartVertex(), graph()));
	case IN:
	    return IteratorUtils.of(new DuctileVertex(this.getBaseEdge().getTargetVertex(), graph()));
	case BOTH:
	    return IteratorUtils.of(new DuctileVertex(this.getBaseEdge().getStartVertex(), graph()),
		    new DuctileVertex(this.getBaseEdge().getTargetVertex(), graph()));
	default:
	    throw new IllegalArgumentException("Direction '" + direction + "' is not supported.");
	}
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
	List<Property<V>> properties = new ArrayList<>();
	for (String key : propertyKeys) {
	    @SuppressWarnings("unchecked")
	    V value = (V) baseEdge.getProperty(key);
	    if (value != null) {
		properties.add(new DuctileProperty<V>(this, key, value));
	    }
	}
	return properties.iterator();
    }

    @Override
    public <V> Property<V> property(final String key) {
	graph().tx().readWrite();
	V value = baseEdge.getProperty(key);
	if (value != null)
	    return new DuctileProperty<>(this, key, value);
	else
	    return Property.empty();
    }

    @Override
    public <V> Property<V> property(String key, V value) {
	ElementHelper.validateProperty(key, value);
	graph().tx().readWrite();
	try {
	    baseEdge.setProperty(key, value);
	    return new DuctileProperty<>(this, key, value);
	} catch (final IllegalArgumentException e) {
	    throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
	}
    }

    @Override
    public DuctileDBEdge getBaseEdge() {
	return baseEdge;
    }

    @Override
    public String label() {
	return baseEdge.getType();
    }

    @Override
    public void remove() {
	graph().tx().readWrite();
	baseEdge.remove();
    }

    @Override
    public boolean equals(final Object object) {
	return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
	return ElementHelper.hashCode(this);
    }

}

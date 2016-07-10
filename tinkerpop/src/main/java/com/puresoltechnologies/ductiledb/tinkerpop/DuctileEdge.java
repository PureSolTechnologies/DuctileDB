package com.puresoltechnologies.ductiledb.tinkerpop;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.puresoltechnologies.ductiledb.api.graph.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.graph.tx.TransactionType;

public class DuctileEdge extends DuctileElement implements Edge, WrappedEdge<DuctileDBEdge> {

    private DuctileDBEdge baseEdge;

    public DuctileEdge(DuctileDBEdge baseEdge, DuctileGraph graph) {
	super(baseEdge, graph);
	this.baseEdge = baseEdge;
    }

    @Override
    public DuctileVertex outVertex() {
	return new DuctileVertex(getBaseEdge().getStartVertex(), graph());
    }

    @Override
    public DuctileVertex inVertex() {
	return new DuctileVertex(getBaseEdge().getTargetVertex(), graph());
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
	if (propertyKeys.length > 0) {
	    for (String key : propertyKeys) {
		@SuppressWarnings("unchecked")
		V value = (V) getBaseEdge().getProperty(key);
		if (value != null) {
		    properties.add(new DuctileProperty<V>(this, key, value));
		}
	    }
	} else {
	    for (String key : getBaseEdge().getPropertyKeys()) {
		@SuppressWarnings("unchecked")
		V value = (V) getBaseEdge().getProperty(key);
		if (value != null) {
		    properties.add(new DuctileProperty<V>(this, key, value));
		}
	    }
	}
	return properties.iterator();
    }

    @Override
    public <V> Property<V> property(final String key) {
	graph().tx().readWrite();
	V value = getBaseEdge().getProperty(key);
	if (value == null) {
	    return Property.empty();

	}
	return new DuctileProperty<>(this, key, value);
    }

    @Override
    public <V> Property<V> property(String key, V value) {
	ElementHelper.validateProperty(key, value);
	graph().tx().readWrite();
	try {
	    if (value == null) {
		getBaseEdge().removeProperty(key);
	    } else {
		getBaseEdge().setProperty(key, value);
	    }
	    return new DuctileProperty<>(this, key, value);
	} catch (final IllegalArgumentException e) {
	    throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
	}
    }

    @Override
    public DuctileDBEdge getBaseEdge() {
	if ((!baseEdge.getTransaction().isOpen())
		&& (baseEdge.getTransaction().getTransactionType() == TransactionType.THREAD_LOCAL)) {
	    /*
	     * The transaction is closed already, so we reconnect this edge to a
	     * new base edge. This is requested by Tinkerpop tests.
	     */
	    baseEdge = graph().getBaseGraph().getEdge(baseEdge.getId());
	}
	return baseEdge;
    }

    @Override
    public String label() {
	return getBaseEdge().getType();
    }

    @Override
    public void remove() {
	graph().tx().readWrite();
	getBaseEdge().remove();
    }

    @Override
    public boolean equals(final Object object) {
	return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
	return ElementHelper.hashCode(this);
    }

    @Override
    public String toString() {
	return StringFactory.edgeString(this);
    }
}

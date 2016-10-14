package com.puresoltechnologies.ductiledb.tinkerpop;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedElement;

import com.puresoltechnologies.ductiledb.core.graph.DuctileDBElement;

public abstract class DuctileElement implements Element, WrappedElement<DuctileDBElement> {

    private final DuctileDBElement baseElement;
    private final DuctileGraph graph;

    public DuctileElement(DuctileDBElement baseElement, DuctileGraph graph) {
	super();
	this.baseElement = baseElement;
	this.graph = graph;
    }

    @Override
    public final DuctileGraph graph() {
	return graph;
    }

    @Override
    public final Long id() {
	graph.tx().readWrite();
	return baseElement.getId();
    }

    @Override
    public final Set<String> keys() {
	graph.tx().readWrite();
	final Set<String> keys = new HashSet<>();
	for (final String key : this.baseElement.getPropertyKeys()) {
	    if (!Graph.Hidden.isHidden(key))
		keys.add(key);
	}
	return Collections.unmodifiableSet(keys);
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
    public final DuctileDBElement getBaseElement() {
	return baseElement;
    }
}

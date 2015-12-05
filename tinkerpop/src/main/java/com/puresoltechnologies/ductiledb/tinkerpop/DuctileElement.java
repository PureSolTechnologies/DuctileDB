package com.puresoltechnologies.ductiledb.tinkerpop;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedElement;

import com.puresoltechnologies.ductiledb.api.DuctileDBElement;

public abstract class DuctileElement implements Element, WrappedElement<DuctileDBElement> {

    private final DuctileDBElement baseElement;
    private final DuctileGraph graph;

    public DuctileElement(DuctileDBElement baseElement, DuctileGraph graph) {
	super();
	this.baseElement = baseElement;
	this.graph = graph;
    }

    @Override
    public final Long id() {
	return baseElement.getId();
    }

    @Override
    public final DuctileGraph graph() {
	return graph;
    }

    @Override
    public final DuctileDBElement getBaseElement() {
	return baseElement;
    }
}
